package operator

import (
	"fmt"
	"net"
	"time"

	"github.com/golang/glog"
)

type Operator interface {
	// Sets the ID of the operator
	SetID(string)

	// Returns the ID of the operator
	GetID() string

	// Creates a bytestream server that other systems can create links to via the
	// Link function.
	Serve(port int) error

	// Creates the bytestream client. Every connection made
	// to that local port will get forwarded to the operator node
	// at the host destination
	LinkAndServe(port int, host string) error

	// Creates a listener that will accept tcp connections
	// from the Dial call with the same channelKey
	RegisterListener(localhost string, remotehost string, channelKey string) error

	// Creates a short-lived tcp connection between the server and the
	// operator node. Everything that goes through this connection
	// will be forwarded to the listener on the other end
	// This will be used by applications that wish to use the bytestream system
	// provided by operator
	Dial(host string, receiverId string, channelKey string) (net.Conn, error)
}

// This can be overridden for mocking and stubbing
var DefaultOperator Operator = nil

// Set the default operator on init
func init() {
	o := &operator{"operator"}
	DefaultOperator = o
}

func Serve(port int) error {
	return DefaultOperator.Serve(port)
}

func LinkAndServe(port int, host string) error {
	return DefaultOperator.LinkAndServe(port, host)
}

func RegisterListener(localhost string, remotehost string, channelKey string) error {
	return DefaultOperator.RegisterListener(localhost, remotehost, channelKey)
}

func Dial(host string, receiverId string, channelKey string) (net.Conn, error) {
	return DefaultOperator.Dial(host, receiverId, channelKey)
}

// Implementation of the default operator
type operator struct {
	ID string
}

func (o *operator) SetID(id string) {
	o.ID = id
}

func (o *operator) GetID() string {
	return o.ID
}

func (o *operator) Serve(port int) error {
	addr := fmt.Sprintf(":%d", port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	glog.V(2).Infof("Serving on port %d", port)
	for {
		conn, err := lis.Accept()
		if err != nil {
			// TODO Do something with that?
			glog.Warningf("Failed to accept connection: %v", err)
			continue
		}
		glog.V(2).Infof("Accepted connection")

		go func() {
			err = o.respond(conn)
			if err != nil {
				glog.Warningf("Failed to respond to connection: %v", err)
				conn.Close()
				return
			}
			glog.V(2).Infof("Successfully handled connection")
		}()
	}
	return nil
}

func (o *operator) LinkAndServe(port int, host string) error {
	receiverId := o.GetID()
	// Link and maintain
	go func() {
		for {
			conn, err := net.Dial("tcp", host)
			if err != nil {
				glog.Errorf("Failed to link to %s: %v. Retrying...", host, err)
				time.Sleep(2 * time.Second)
				continue
			}

			glog.V(2).Infof("Linking to %s as %s", host, receiverId)

			// Send the link request
			req := &LinkRequest{receiverId}
			err = SendFrame(conn, req)
			if err != nil {
				glog.V(1).Infof("Broken link to %s as %s: %vRetrying...", host, receiverId, err)
				continue
			}

			resp, err := GetFrame(conn)
			if err != nil {
				glog.V(1).Infof("Broken link to %s as %s: %vRetrying...", host, receiverId, err)
				continue
			}

			cast, ok := resp.(*LinkResponse)
			if !ok || resp.IsError() {
				glog.V(1).Infof("Broken link to %s as %s: %vRetrying...", host, receiverId, err)
				continue
			}

			DefaultConnectionManager.SetLink(cast.receiverID, conn)

			err = SendHeartbeats(conn) // Blocks
			glog.V(1).Infof("Broken link to %s as %s: %vRetrying...", host, receiverId, err)
			conn.Close()
			// TODO remove link
		}
	}()

	// Serve
	return Serve(port)
}

func (o *operator) Dial(host string, receiverID string, serviceKey string) (net.Conn, error) {
	// Dial the operator
	conn, err := net.Dial("tcp", host)
	if err != nil {
		glog.Errorf("Failed to dial operator: %v", err)
		return nil, err
	}
	defer conn.Close()

	glog.V(2).Infof("Dialing operator")

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
	}

	glog.V(2).Infof("Got dial response frame")

	// Make sure it gets a good response
	cast, ok := resp.(*DialResponse)
	if !ok || resp.IsError() {
		err = fmt.Errorf("connection refused from operator")
		glog.Errorf("Failed to dial operator: %v", err)
		return nil, err
	}

	glog.V(2).Infof("Operator returned channel ID: %s", cast.channelID)

	// Create a new connection with operator
	conn, err = net.Dial("tcp", host)
	if err != nil {
		glog.Errorf("Failed to dial operator: %v", err)
		return nil, err
	}

	channel := NewChannel(conn, receiverID, cast.channelID)
	return channel, nil
}

func (o *operator) RegisterListener(serviceHost, remotehost string, serviceKey string) error {
	// Dial the operator
	conn, err := net.Dial("tcp", remotehost)
	if err != nil {
		glog.Errorf("Failed to dial operator: %v", err)
		return err
	}

	glog.Infof("Registering listener: %s (%s)", serviceHost, serviceKey)

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
		glog.Errorf("Failed to dial operator: %v", err)
		return err
	}

	// Make sure it gets a good response
	resp, ok := f.(*RegisterResponse)
	if !ok || resp.IsError() {
		err = fmt.Errorf("connection refused from operator: '%s'", resp.String())
		glog.Errorf("Failed to register with operator: %v", err)
		return err
	}

	glog.Infof("Successfully registered listener: %s (%s)", serviceHost, serviceKey)
	return nil
}

func (o *operator) respond(conn net.Conn) error {
	f, err := GetFrame(conn)
	if err != nil {
		return err
	}

	glog.V(2).Infof("Got header: %d", f.Header())
	glog.V(2).Infof("Got content: %s", string(f.Content()))

	// Handle this frame
	err = o.handleFrame(conn, f)
	if err != nil {
		return err
	}
	glog.V(2).Infof("Successfully handled frame")
	return nil
}

func (o *operator) handleFrame(conn net.Conn, f Frame) error {
	switch f.Header() {
	case HEADER_LINK_REQ:
		req, ok := f.(*LinkRequest)
		resp := &LinkResponse{o.ID}
		if !ok || req.IsError() {
			// Should never happen
			return SendFrame(conn, resp.SetError())
		}

		glog.V(2).Infof("Link request: %s", req.String())
		DefaultConnectionManager.SetLink(req.receiverID, conn)
		return SendFrame(conn, resp)

	case HEADER_REGISTER_REQ:
		req, ok := f.(*RegisterRequest)
		resp := &RegisterResponse{}
		if !ok || req.IsError() {
			// Should never happen
			return SendFrame(conn, resp.SetError())
		}

		glog.V(2).Infof("Register request %s", f.String())
		DefaultConnectionManager.SetService(req.serviceKey, req.serviceHost)
		return SendFrame(conn, resp)

	case HEADER_DIAL_REQ:
		req, ok := f.(*DialRequest)
		resp := &DialResponse{}
		if !ok || req.IsError() {
			// Should never happen
			return SendFrame(conn, resp.SetError())
		}

		glog.V(2).Infof("Dial request to %s", f.String())

		l := DefaultConnectionManager.GetLink(req.receiverID)
		if l == nil {
			return SendFrame(conn, resp.SetError())
		}

		ID := <-l.Tunnel(req.serviceKey)
		if ID == TUNNEL_ERR {
			return SendFrame(conn, resp.SetError())
		}
		resp.channelID = ID
		return SendFrame(conn, resp)

	case HEADER_DATA:
		req, ok := f.(*DataFrame)
		if !ok || req.IsError() {
			// Should never happen
			return nil // TODO send error frame of some sort?
		}
		glog.V(2).Infof("Data frame: %s", req.String())

		l := DefaultConnectionManager.GetLink(req.receiverID)
		if l == nil {
			return fmt.Errorf("Failed to get link to %s", req.receiverID)
		}

		glog.V(2).Infof("Forwarding data frame: %s", req.String())
		return SendFrame(l, req)
	}
	return nil
}
