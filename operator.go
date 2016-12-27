package operator

import (
	"fmt"
	"net"
	"time"

	"context"

	"github.com/golang/glog"
)

type OperatorInterface interface {
	// Sets the ID of the operator and returns itself
	SetID(string) OperatorInterface

	// Returns the ID of the operator
	GetID() string

	// Creates a bytestream server that other systems can create links to via the
	// Link function.
	Serve(port int) error

	// Creates the bytestream client. Every connection made
	// to that local port will get forwarded to the operator node
	// at the host destination
	LinkAndServe(port int, operatorAddr string) error

	// Creates a listener that will accept tcp connections
	// from the Dial call with the same channelKey
	RegisterListener(serviceAddr string, operatorAddr string, channelKey string) error

	// Creates a short-lived tcp connection between the server and the
	// operator node. Everything that goes through this connection
	// will be forwarded to the listener on the other end
	// This will be used by applications that wish to use the bytestream system
	// provided by operator
	Dial(receiverId string, channelKey string) (net.Conn, error)
}

func NewOperator() *Operator {
	o := &Operator{}
	o.ReceiverID = "operator"
	o.OperatorResolver = DefaultOperatorResolver
	o.ConnectionManager = DefaultConnectionManager
	return o
}

type Operator struct {
	ReceiverID        string
	ConnectionManager ConnectionManager
	OperatorResolver  OperatorResolver
}

func (o *Operator) SetID(id string) *Operator {
	o.ReceiverID = id
	return o
}

func (o *Operator) GetID() string {
	return o.ReceiverID
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
			err := o.respond(conn)
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
				time.Sleep(2 * time.Second)
				continue
			}
			defer conn.Close()

			// Send the link request
			req := &LinkRequest{receiverId}
			err = SendFrame(conn, req)
			if err != nil {
				glog.Warningf("Broken link to %s as %s: %vRetrying...", host, receiverId, err)
				continue
			}

			// Check the response is good
			resp, err := GetFrame(conn)
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
			o.ConnectionManager.SetLink(cast.receiverID, conn)

			// Send heartbeats until it closes
			err = SendHeartbeats(conn) // Blocks
			glog.Warningf("Broken link to %s as %s: %vRetrying...", host, receiverId, err)

			o.ConnectionManager.RemoveLink(cast.receiverID)
		}
	}()

	// Serve
	return o.Serve(port)
}

func (o *Operator) Dial(receiverID string, serviceKey string) (net.Conn, error) {
	glog.V(3).Infof("Operator Dialing...")

	// Use the OperatorResolver to find the right operator
	host, err := o.OperatorResolver.ResolveOperator(receiverID)
	if err != nil {
		glog.Errorf("OperatorResolver error: %v", err)
		return nil, err
	}
	glog.V(1).Infof("Resolved receiverID to operator at: %s", host)

	// Dial the operator
	conn, err := net.Dial("tcp", host)
	if err != nil {
		glog.Errorf("Failed to dial operator: %v", err)
		return nil, err
	}

	// Send the request
	req := &DialRequest{receiverID, serviceKey}
	err = SendFrame(conn, req)
	if err != nil {
		glog.Errorf("Failed to dial operator: %v", err)
		return nil, err
	}

	// Read the response frame
	resp, err := GetFrame(conn)
	if err != nil {
		glog.Errorf("Failed to dial operator: %v", err)
		return nil, err
	} else if resp.IsError() {
		glog.Errorf("Failed to dial operator: %s", string(resp.Content()))
		return nil, fmt.Errorf(string(resp.Content()))
	}

	// Make sure it gets a good response
	cast, ok := resp.(*DialResponse)
	if !ok {
		return nil, ImpossibleError()
	}

	// Done!
	glog.V(3).Infof("Operator dialed. Channel ID: %s", cast.channelID)
	return conn, nil
}

func (o *Operator) RegisterListener(serviceHost, remotehost string, serviceKey string) error {
	glog.V(3).Infof("Registering operator service...")
	// Dial the operator
	conn, err := net.Dial("tcp", remotehost)
	if err != nil {
		glog.Errorf("Failed to dial operator: %v", err)
		return err
	}

	// Send register request
	req := &RegisterRequest{serviceHost, serviceKey}
	err = SendFrame(conn, req)
	if err != nil {
		glog.Errorf("Failed to register with operator: %v", err)
		return err
	}

	// Read the response frame
	f, err := GetFrame(conn)
	if err != nil {
		glog.Errorf("Failed to register service: %v", err)
		return err
	} else if f.IsError() {
		glog.Errorf("Failed to register service: %s", string(f.Content()))
		return err
	}

	// Done!
	glog.V(2).Infof("Successfully registered service: %s (%s)", serviceHost, serviceKey)
	conn.Close()

	return nil
}

func (o *Operator) DialContext(receiverID string, channelKey string) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, _ string) (net.Conn, error) {
		return o.Dial(receiverID, channelKey)
	}
}

func (o *Operator) respond(conn net.Conn) error {
	f, err := GetFrame(conn)
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

func (o *Operator) handleLinkRequest(conn net.Conn, req *LinkRequest) error {
	glog.V(2).Infof("Link request: %s", req.String())
	o.ConnectionManager.SetLink(req.receiverID, conn)

	resp := &LinkResponse{o.GetID()}
	return SendFrame(conn, resp)
}

func (o *Operator) handleRegisterRequest(conn net.Conn, req *RegisterRequest) error {
	glog.V(2).Infof("Register request %s", req.String())
	o.ConnectionManager.SetService(req.serviceKey, req.serviceHost)

	resp := &RegisterResponse{}
	return SendFrame(conn, resp)
}

func (o *Operator) handleDialRequest(conn net.Conn, req *DialRequest) error {
	glog.V(2).Infof("Dial request to %s", req.String())
	l := o.ConnectionManager.GetLink(req.receiverID)
	if l == nil {
		glog.Warningf("Link not found: %s", req.receiverID)
		return SendFrame(conn, &ErrorFrame{"Link not found"})
	}

	frame := <-l.Tunnel(req.serviceKey)
	res, ok := frame.(*DialResponse)
	if frame.IsError() || !ok {
		glog.Warningf("Dial error received from tunnel: %v", string(frame.Content()))
		return SendFrame(conn, &ErrorFrame{"Service discovery failed: " + string(frame.Content())})
	}

	l.CreatePipe(res.channelID, conn)
	l.PipeIn(res.channelID, conn)

	resp := &DialResponse{res.channelID}
	return SendFrame(conn, resp)
}

func (o *Operator) handleFrame(conn net.Conn, f Frame) error {
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
