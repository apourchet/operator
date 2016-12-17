package operator

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/golang/glog"
)

type Operator interface {
	// Creates a bytestream server that other systems can create links to via the
	// Link function.
	Serve(port int) error

	// Creates the bytestream client. Every connection made
	// to that local port will get forwarded to the operator node
	// at the host destination
	LinkAndServe(port int, host string, receiverId string) error

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
	o := &operator{}
	DefaultOperator = o
}

func Serve(port int) error {
	return DefaultOperator.Serve(port)
}

func LinkAndServe(port int, host string, receiverId string) error {
	return DefaultOperator.LinkAndServe(port, host, receiverId)
}

func RegisterListener(localhost string, remotehost string, channelKey string) error {
	return DefaultOperator.RegisterListener(localhost, remotehost, channelKey)
}

func Dial(host string, receiverId string, channelKey string) (net.Conn, error) {
	return DefaultOperator.Dial(host, receiverId, channelKey)
}

// Implementation of the default operator
type operator struct{}

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
				return
			}
			glog.V(2).Infof("Successfully handled connection")
		}()
	}
	return nil
}

func (o *operator) LinkAndServe(port int, host string, receiverId string) error {
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
			SendLinkRequest(conn, receiverId)

			// Read the response frame
			h, _, err := getFrame(conn)
			if err != nil {
				glog.V(1).Infof("Broken link to %s as %s: %v\nRetrying...", host, receiverId, err)
				continue
			}

			// Check the LINK_RES
			if h != HEADER_LINK_RES {
				glog.V(1).Infof("Broken link to %s as %s: %v\nRetrying...", host, receiverId, fmt.Errorf("Expected HEADER_LINK_RES, got %d", h))
				continue
			}

			// TODO Use content
			err = SendHeartbeats(conn) // Blocks
			glog.V(1).Infof("Broken link to %s as %s: %v\nRetrying...", host, receiverId, err)
		}
	}()

	// Serve
	return Serve(port)
}

func (o *operator) Dial(host string, receiverID string, channelKey string) (net.Conn, error) {
	// Dial the operator
	conn, err := net.Dial("tcp", host)
	if err != nil {
		glog.Errorf("Failed to dial operator: %v", err)
		return nil, err
	}
	defer conn.Close()

	glog.V(2).Infof("Dialing operator")

	// Send the request
	err = SendDialRequest(conn, receiverID, channelKey)
	if err != nil {
		glog.Errorf("Failed to dial operator: %v", err)
		return nil, err
	}

	// Read the response frame
	h, content, err := getFrame(conn)
	if err != nil {
		glog.Errorf("Failed to dial operator: %v", err)
		return nil, err
	}
	glog.V(2).Infof("Got dial response frame")

	// Make sure it gets a good response
	if h != HEADER_DIAL_RES || content == DIAL_ERROR {
		err = fmt.Errorf("connection refused from operator")
		glog.Errorf("Failed to dial operator: %v", err)
		return nil, err
	}

	glog.V(2).Infof("Operator returned channel ID: %s", content)

	// Create a new connection with operator
	conn, err = net.Dial("tcp", host)
	if err != nil {
		glog.Errorf("Failed to dial operator: %v", err)
		return nil, err
	}

	channel := NewChannel(conn, content)
	return channel, nil
}

func (o *operator) RegisterListener(serviceHost, remotehost string, channelKey string) error {
	// Dial the operator
	conn, err := net.Dial("tcp", remotehost)
	if err != nil {
		glog.Errorf("Failed to dial operator: %v", err)
		return err
	}

	glog.Infof("Registering listener: %s (%s)", serviceHost, channelKey)

	// Send the request
	err = SendRegisterRequest(conn, serviceHost, channelKey)
	if err != nil {
		glog.Errorf("Failed to dial operator: %v", err)
		return err
	}

	// Read the response frame
	h, content, err := getFrame(conn)
	if err != nil {
		glog.Errorf("Failed to dial operator: %v", err)
		return err
	}

	// Make sure it gets a good response
	if h != HEADER_REGISTER_RES || content != REGISTER_OK {
		err = fmt.Errorf("connection refused from operator: '%s'", content)
		glog.Errorf("Failed to dial operator: %v", err)
		return err
	}

	glog.Infof("Successfully registered listener: %s (%s)", serviceHost, channelKey)
	return nil
}

func (o *operator) respond(conn net.Conn) error {
	h, content, err := getFrame(conn)
	if err != nil {
		return err
	}

	glog.V(2).Infof("Got header: %d", h)
	glog.V(2).Infof("Got content: %s", content)

	// Handle this frame
	err = o.handleFrame(conn, h, content)
	if err != nil {
		return err
	}
	glog.V(2).Infof("Successfully handled frame")
	return nil
}

func (o *operator) handleFrame(conn net.Conn, h byte, content string) error {
	switch h {
	case HEADER_LINK_REQ:
		glog.V(2).Infof("Link request from: %s", content)
		DefaultConnectionManager.SetLink(content, conn)
		return SendLinkResponse(conn, "operator") // TODO real ID
	case HEADER_REGISTER_REQ:
		glog.V(2).Infof("Register request from: %s", content)
		split := strings.Split(content, ",")
		if len(split) != 2 {
			return fmt.Errorf("Malformed register frame")
		}
		serviceName, host := split[0], split[1]
		DefaultConnectionManager.SetService(serviceName, host)
		return SendRegisterResponse(conn)
	case HEADER_DIAL_REQ:
		glog.V(2).Infof("Dial request to: %s", content)
		split := strings.Split(content, ",")
		if len(split) != 2 {
			return fmt.Errorf("Malformed dial frame")
		}
		l := DefaultConnectionManager.GetLink(split[0])
		if l == nil {
			return SendDialResponse(conn, DIAL_ERROR)
		}
		serviceName := split[1]
		ID := <-l.Tunnel(serviceName)
		if ID == "" {
			return SendDialResponse(conn, DIAL_ERROR)
		}
		return SendDialResponse(conn, ID)
	case HEADER_DATA:
		split := strings.Split(content, ",")
		if len(split) != 2 {
			glog.Warningf("Malformed data frame")
			return fmt.Errorf("Malformed data frame")
		}
		glog.V(2).Infof("Data frame: %s", split[0])

		conn, err := net.Dial("tcp", "localhost:10002")
		if err != nil {
			glog.Errorf("Failed to dial service: %v", err)
			return err
		}

		return nil
	}
	return nil
}
