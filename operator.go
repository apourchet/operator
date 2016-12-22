package operator

import (
	"fmt"
	"net"
	"time"

	"context"

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

// Thin wrapper around Dial to implement http.Transport.DialContext
func DialContext(receiverID string, channelKey string) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return DefaultOperator.Dial(addr, receiverID, channelKey)
	}
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

func (o *operator) LinkAndServe(port int, host string) error {
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
			DefaultConnectionManager.SetLink(cast.receiverID, conn)

			// Send heartbeats until it closes
			err = SendHeartbeats(conn) // Blocks
			glog.Warningf("Broken link to %s as %s: %vRetrying...", host, receiverId, err)

			DefaultConnectionManager.RemoveLink(cast.receiverID)
		}
	}()

	// Serve
	return Serve(port)
}

func (o *operator) Dial(host string, receiverID string, serviceKey string) (net.Conn, error) {
	glog.V(3).Infof("Operator Dialing...")

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

func (o *operator) RegisterListener(serviceHost, remotehost string, serviceKey string) error {
	glog.V(3).Infof("Registering operator service...")
	go func() {
		for {
			// Dial the operator
			conn, err := net.Dial("tcp", remotehost)
			if err != nil {
				glog.Errorf("Failed to dial operator: %v", err)
				time.Sleep(5 * time.Second)
				continue
			}

			// Send register request
			req := &RegisterRequest{serviceHost, serviceKey}
			err = SendFrame(conn, req)
			if err != nil {
				glog.Errorf("Failed to register with operator: %v", err)
				time.Sleep(2 * time.Second)
				continue
			}

			// Read the response frame
			f, err := GetFrame(conn)
			if err != nil {
				glog.Errorf("Failed to register service: %v", err)
				time.Sleep(2 * time.Second)
				continue
			} else if f.IsError() {
				glog.Errorf("Failed to register service: %s", string(f.Content()))
				time.Sleep(2 * time.Second)
				continue
			}

			// Done!
			glog.V(2).Infof("Successfully registered service: %s (%s)", serviceHost, serviceKey)
			conn.Close()
			time.Sleep(10 * time.Second)
		}
	}()

	return nil
}

func (o *operator) respond(conn net.Conn) error {
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

func (o *operator) handleFrame(conn net.Conn, f Frame) error {
	switch f.Header() {
	case HEADER_LINK_REQ:
		req, ok := f.(*LinkRequest)
		if !ok {
			return ImpossibleError()
		}

		glog.V(2).Infof("Link request: %s", req.String())
		DefaultConnectionManager.SetLink(req.receiverID, conn)

		resp := &LinkResponse{o.ID}
		return SendFrame(conn, resp)

	case HEADER_REGISTER_REQ:
		req, ok := f.(*RegisterRequest)
		resp := &RegisterResponse{}
		if !ok {
			return ImpossibleError()
		}

		glog.V(2).Infof("Register request %s", f.String())
		DefaultConnectionManager.SetService(req.serviceKey, req.serviceHost)
		return SendFrame(conn, resp)

	case HEADER_DIAL_REQ:
		req, ok := f.(*DialRequest)
		if !ok {
			return ImpossibleError()
		}

		glog.V(2).Infof("Dial request to %s", f.String())
		l := DefaultConnectionManager.GetLink(req.receiverID)
		if l == nil {
			glog.Warningf("Link not found: %s", req.receiverID)
			return SendFrame(conn, &ErrorFrame{"Link not found"})
		}

		ID := <-l.Tunnel(req.serviceKey)
		if ID == TUNNEL_ERR {
			glog.Warningf("Link not found: %s", req.receiverID)
			return SendFrame(conn, &ErrorFrame{"Service discovery failed"})
		}

		l.CreatePipe(ID, conn)
		l.PipeIn(ID, conn)

		resp := &DialResponse{ID}
		return SendFrame(conn, resp)
	}
	return nil
}
