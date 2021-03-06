package operator

import (
	"fmt"
	"math/rand"
	"net"
	"time"

	"github.com/golang/glog"
)

type OperatorInterface interface {
	// Sets the ID of the operator and returns itself
	SetID(string) OperatorInterface

	// Returns the ID of the operator
	GetID() string

	// Sets this operator's address to <addr> that the OperatorResolver will use
	// When it creates a link, it calls OperatorResolver.SetOperator(<recID>, <addr>)
	SetAddress(string) error

	// Creates a bytestream server that other systems can create links to via the
	// Link function.
	Serve(port int) OperatorInterface

	// Creates the bytestream client. Every connection made
	// to that local port will get forwarded to the operator node
	// at the host destination
	LinkAndServe(port int, operatorAddr string) error
}

func NewOperator(receiverID, address string) *Operator {
	o := &Operator{}
	o.ReceiverID = receiverID
	o.Address = address
	o.OperatorResolver = DefaultOperatorResolver
	o.ConnectionManager = DefaultConnectionManager
	o.ServiceResolver = DefaultServiceResolver
	return o
}

type Operator struct {
	ReceiverID        string
	Address           string
	ConnectionManager ConnectionManager
	OperatorResolver  OperatorResolver
	ServiceResolver   ServiceResolver
}

func (o *Operator) SetID(id string) *Operator {
	o.ReceiverID = id
	return o
}

func (o *Operator) GetID() string {
	return o.ReceiverID
}

func (o *Operator) SetAddress(address string) *Operator {
	o.Address = address
	return o
}

func (o *Operator) Serve(port int) error {
	glog.V(1).Infof("Serving operator on port %d", port)
	addr := fmt.Sprintf(":%d", port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		glog.Errorf("Failed to serve operator: %v", err)
		return err
	}

	for {
		conn, err := lis.Accept()
		if err != nil {
			// TODO Do something with that?
			glog.Warningf("Failed to accept connection: %v", err)
			continue
		}
		glog.V(3).Infof("Accepted connection")

		go func() {
			err := o.respond(NewBufferedConnection(conn))
			if err != nil {
				glog.Warningf("Failed to respond to connection: %v", err)
				return
			}
			glog.V(2).Infof("Successfully handled connection")
		}()
	}
	return nil
}

func (o *Operator) LinkAndServe(port int, host string) error {
	receiverId := o.GetID()

	// Try to keep link alive
	go func() {
		for {
			glog.V(3).Infof("Linking to %s as %s", host, receiverId)
			conn, err := net.Dial("tcp", host)
			if err != nil {
				glog.Errorf("Failed to link to %s: %v. Retrying...", host, err)
				time.Sleep(time.Duration(1000+rand.Int31n(3000)) * time.Millisecond)
				continue
			}
			defer conn.Close()

			bufConn := NewBufferedConnection(conn)

			// Send the link request
			req := &LinkRequest{receiverId}
			_, err = bufConn.SendFrame(req)
			if err != nil {
				glog.Warningf("Broken link to %s as %s: %vRetrying...", host, receiverId, err)
				continue
			}

			// Check the response is good
			resp, err := bufConn.GetFrame()
			if err != nil {
				glog.Warningf("Broken link to %s as %s: %vRetrying...", host, receiverId, err)
				continue
			} else if resp.IsError() {
				glog.Warningf("Broken link to %s as %s: %sRetrying...", host, receiverId, string(resp.Content()))
				continue
			}

			// Cast to get receiverID
			cast, ok := resp.(*LinkResponse)
			if !ok {
				glog.Warningf("Broken link to %s as %s: %vRetrying...", host, receiverId, ImpossibleError())
				continue
			}

			// Set and maintain that link
			o.ConnectionManager.SetLink(cast.receiverID, bufConn)
			err = o.OperatorResolver.SetOperator(cast.receiverID, o.Address)
			if err != nil {
				glog.Warningf("OperatorResolver error: %v", err)
			}

			glog.V(2).Infof("Linked to %s as %s", cast.receiverID, receiverId)

			// Send heartbeats until it closes
			err = SendHeartbeats(bufConn) // Blocks
			glog.Warningf("Broken link to %s as %s: %vRetrying...", host, receiverId, err)

			o.ConnectionManager.RemoveLink(cast.receiverID)
		}
	}()

	// Serve
	return o.Serve(port)
}

func (o *Operator) respond(conn FrameReadWriter) error {
	// Get the outstanding frame from that connection
	f, err := conn.GetFrame()
	if err != nil {
		return err
	}

	if f.IsError() {
		return fmt.Errorf("%s", string(f.Content()))
	}

	// Handle this frame
	err = o.handleFrame(conn, f)
	if err != nil {
		return err
	}
	return nil
}

func (o *Operator) handleLinkRequest(conn FrameReadWriter, req *LinkRequest) error {
	glog.V(2).Infof("Link request: %s", req.String())

	resp := &LinkResponse{o.GetID()}
	_, err := conn.SendFrame(resp)
	if err != nil {
		return err
	}
	o.ConnectionManager.SetLink(req.receiverID, conn)

	return o.OperatorResolver.SetOperator(req.receiverID, o.Address)
}

func (o *Operator) handleRegisterRequest(conn FrameReadWriter, req *RegisterRequest) error {
	glog.V(2).Infof("Register request %s", req.String())
	o.ServiceResolver.SetService(req.serviceKey, req.serviceHost)

	resp := &RegisterResponse{}
	_, err := conn.SendFrame(resp)
	return err
}

func (o *Operator) handleDialRequest(conn FrameReadWriter, req *DialRequest) error {
	glog.V(2).Infof("Dial request to %s", req.String())
	l, err := o.ConnectionManager.GetLink(req.receiverID)
	if err != nil {
		glog.Warningf("Failed to get link %s: %v", req.receiverID, err)
		_, err := conn.SendFrame(&ErrorFrame{err.Error()})
		return err
	}

	frame := <-l.Tunnel(req.serviceKey)
	res, ok := frame.(*DialResponse)
	if frame.IsError() || !ok {
		glog.Warningf("Dial error received from tunnel: %v", string(frame.Content()))
		_, err := conn.SendFrame(&ErrorFrame{"Service discovery failed: " + string(frame.Content())})
		return err
	}

	l.CreatePipe(res.channelID, conn)
	l.PipeIn(res.channelID, conn)

	resp := &DialResponse{res.channelID}
	_, err = conn.SendFrame(resp)
	return err
}

func (o *Operator) handleFrame(conn FrameReadWriter, f Frame) error {
	switch f.Header() {
	case HEADER_LINK_REQ:
		req, ok := f.(*LinkRequest)
		if !ok {
			return ImpossibleError()
		}
		return o.handleLinkRequest(conn, req)

	case HEADER_REGISTER_REQ:
		req, ok := f.(*RegisterRequest)
		if !ok {
			return ImpossibleError()
		}
		return o.handleRegisterRequest(conn, req)

	case HEADER_DIAL_REQ:
		req, ok := f.(*DialRequest)
		if !ok {
			return ImpossibleError()
		}
		return o.handleDialRequest(conn, req)
	}

	return fmt.Errorf("Unrecognized header: %d", f.Header())
}

// Creates a listener that will accept tcp connections
// from the Dial call with the same channelKey
func RegisterService(operatorAddr, serviceKey, serviceAddr string) error {
	glog.V(3).Infof("Registering operator service...")

	// Dial the operator
	conn, err := net.Dial("tcp", operatorAddr)
	if err != nil {
		glog.Errorf("Failed to dial operator: %v", err)
		return err
	}

	bufConn := NewBufferedConnection(conn)

	// Send register request
	req := &RegisterRequest{serviceAddr, serviceKey}
	_, err = bufConn.SendFrame(req)
	if err != nil {
		glog.Errorf("Failed to register with operator: %v", err)
		return err
	}

	// Read the response frame
	f, err := bufConn.GetFrame()
	if err != nil {
		glog.Errorf("Failed to register service: %v", err)
		return err
	} else if f.IsError() {
		glog.Errorf("Failed to register service: %s", string(f.Content()))
		return err
	}

	// Done!
	glog.V(2).Infof("Successfully registered service: %s (%s)", serviceAddr, serviceKey)
	conn.Close()

	return nil
}
