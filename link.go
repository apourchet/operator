package operator

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/golang/glog"
)

type Link struct {
	LastHeartbeat  time.Time
	TunnelsWaiting map[string]chan Frame
	Pipes          map[string]net.Conn
	ReceiverID     string
	tunnelLock     sync.Mutex
	net.Conn
}

func NewLink(conn net.Conn, receiverID string) *Link {
	l := Link{time.Now(), map[string]chan Frame{}, map[string]net.Conn{}, receiverID, sync.Mutex{}, conn}
	return &l
}

// Sends down a tunnel request and creates a channel that will receive the response frame
// the caller should wait on the frame that will come as a response
// which will either be a DialResponse or an ErrorFrame
func (l *Link) Tunnel(serviceKey string) chan Frame {
	ID := NewID()
	channel := make(chan Frame, 1)

	l.tunnelLock.Lock()
	l.TunnelsWaiting[ID] = channel
	l.tunnelLock.Unlock()

	glog.V(2).Infof("Tunneling to %s for service %s (%s)", l.ReceiverID, serviceKey, ID)
	req := &TunnelRequest{ID, serviceKey}
	_, err := SendFrame(l, req)
	if err != nil {
		msg := fmt.Sprintf("Unable to send dial through link: %v", err)
		glog.Errorf("Tunneling error: %v", err)
		f := &ErrorFrame{msg}
		channel <- f
	}

	return channel
}

// Loops forever or until the connection gets closed
// Handles every frame that comes through the link
func (l *Link) Maintain() {
	for {
		f, err := GetFrame(l)
		if err != nil && err != io.EOF {
			glog.Warningf("Failed to get frame: %v", err)
			continue
		} else if err == io.EOF {
			glog.Errorf("Link permanently closed: EOF")
			DefaultConnectionManager.RemoveLink(l.ReceiverID)
			return
		}

		// Handle this frame
		err = l.handleFrame(f)
		if err != nil {
			glog.Warningf("Failed to handle frame: %v", err)
			continue
		}
	}
}

// Handles the data frame
// Pipes that out to a listening connection
func (l *Link) handleDataFrame(data *DataFrame) error {
	glog.V(3).Infof("Link got data frame: %s", data.String())
	err := l.PipeOut(data.channelID, data.content)
	if err != nil {
		return err
	}

	glog.V(3).Infof("Successfully handled data frame (%s)", data.channelID)
	return nil
}

func (l *Link) handleTunnelRequest(req *TunnelRequest) error {
	glog.V(3).Infof("Link got tunnel request: %s", req.String())
	serviceHost, found, err := DefaultServiceResolver.GetService(req.serviceKey)
	if err != nil {
		_, err := SendFrame(l, &TunnelErrorFrame{req.channelID, err.Error()})
		return err
	}

	if !found {
		_, err := SendFrame(l, &TunnelErrorFrame{req.channelID, "Service not found"})
		return err
	}

	conn, err := net.Dial("tcp", serviceHost)
	if err != nil {
		glog.Errorf("Failed to dial service %s (%s): %v", req.serviceKey, req.channelID, err)
		_, err := SendFrame(l, &TunnelErrorFrame{req.channelID, "Service connection error: " + err.Error()})
		return err
	}

	l.CreatePipe(req.channelID, conn)
	l.PipeIn(req.channelID, conn)

	// Create response
	res := &TunnelResponse{}
	res.channelID = req.channelID

	glog.V(2).Infof("Successfully handled tunnel request (%s)", req.channelID)
	_, err = SendFrame(l, res)
	return err
}

func (l *Link) handleTunnelResponse(res *TunnelResponse) error {
	glog.V(2).Infof("Link got tunnel response: %s", res.String())
	channel, found := l.TunnelsWaiting[res.channelID]
	if !found {
		glog.Warningf("Tunnel response was found no associated waiting channel: %s", res.channelID)
		return nil
	}

	frame := &DialResponse{res.channelID}
	channel <- frame
	glog.V(2).Infof("Successfully handled tunnel response (%s)", res.channelID)
	return nil
}

func (l *Link) handleTunnelError(res *TunnelErrorFrame) error {
	glog.V(2).Infof("Link got tunnel error: %s", res.String())

	channel, found := l.TunnelsWaiting[res.channelID]
	if !found {
		glog.Warningf("Tunnel response was found no associated waiting channel: %s", res.channelID)
		return nil
	}

	msg := fmt.Sprintf("Tunnel error: %v", res.message)
	frame := &ErrorFrame{msg}
	channel <- frame
	return nil
}

// All data frames with this channelID going through the link
// will be forwarded to this connection
func (l *Link) CreatePipe(channelID string, conn net.Conn) {
	glog.V(2).Infof("Link creating pipe (%s)", channelID)
	l.tunnelLock.Lock()
	defer l.tunnelLock.Unlock()
	l.Pipes[channelID] = conn
}

// Reads data from the connection
// Transforms it into a DataFrame
// Sends DataFrame through the link
func (l *Link) PipeIn(channelID string, conn net.Conn) {
	go func() {
		dest := NewLinkWriter(l, l.ReceiverID, channelID)
		n, err := io.Copy(dest, conn)
		if err != nil && err == io.EOF {
			glog.Warningf("Pipe closed (%s). Wrote %d bytes", channelID, n)
			l.tunnelLock.Lock()
			delete(l.Pipes, channelID)
			l.tunnelLock.Unlock()
			return
		} else if err != nil {
			glog.Warningf("Pipe error (%s): %v", channelID, err)
			return
		}
	}()
}

// Sends the escaped content (coming from a DataFrame) through a
// connection that corresponds to a channelID
func (l *Link) PipeOut(channelID string, content string) error {
	conn, found := l.Pipes[channelID]
	if !found {
		glog.Errorf("Failed PipeOut: pipe not found: %s", channelID)
		return fmt.Errorf("Pipe not found: %s", channelID)
	}
	_, err := conn.Write(UnescapeContent(content))
	return err
}

func (l *Link) handleFrame(f Frame) error {
	switch f.Header() {
	case HEADER_DATA:
		data, ok := f.(*DataFrame)
		if !ok {
			return ImpossibleError()
		}
		return l.handleDataFrame(data)

	case HEADER_TUNNEL_REQ:
		req, ok := f.(*TunnelRequest)
		if !ok {
			return ImpossibleError()
		}
		return l.handleTunnelRequest(req)

	case HEADER_TUNNEL_RES:
		res, ok := f.(*TunnelResponse)
		if !ok {
			return ImpossibleError()
		}
		return l.handleTunnelResponse(res)

	case HEADER_TUNNEL_ERROR:
		res, ok := f.(*TunnelErrorFrame)
		if !ok {
			return ImpossibleError()
		}
		return l.handleTunnelError(res)

	case HEADER_HEARTBEAT:
		l.LastHeartbeat = time.Now()
		return nil
	}

	return fmt.Errorf("Unrecognized header: %d", f.Header())
}
