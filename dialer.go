package operator

import (
	"fmt"
	"net"

	"github.com/golang/glog"

	"context"
)

type DialerInterface interface {
	// Creates a short-lived tcp connection between the server and the
	// operator node. Everything that goes through this connection
	// will be forwarded to the listener on the other end
	// This will be used by applications that wish to use the bytestream system
	// provided by operator
	Dial(receiverId string, channelKey string) (net.Conn, error)

	// For ease of use in the net.http package
	DialContext(receiverID string, channelKey string) func(context.Context, string, string) (net.Conn, error)
}

type Dialer struct {
	OperatorResolver OperatorResolver
}

func NewDialer(resolver OperatorResolver) *Dialer {
	if resolver == nil {
		resolver = DefaultOperatorResolver
	}
	d := &Dialer{resolver}
	return d
}

func (d *Dialer) Dial(receiverID string, serviceKey string) (net.Conn, error) {
	glog.V(3).Infof("Operator Dialing...")

	// Use the OperatorResolver to find the right operator
	host, err := d.OperatorResolver.ResolveOperator(receiverID)
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

func (dialer *Dialer) DialContext(receiverID string, channelKey string) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, _ string) (net.Conn, error) {
		return dialer.Dial(receiverID, channelKey)
	}
}
