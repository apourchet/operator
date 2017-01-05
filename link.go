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
	ReceiverID     string
	tunnelsWaiting map[string]chan Frame
	pipes          map[string]io.Writer
	tunnelLock     sync.Mutex
	stream         FrameReadWriter
}

func NewLink(conn FrameReadWriter, receiverID string) *Link {
	link := Link{}
	link.LastHeartbeat = time.Now()
	link.ReceiverID = receiverID
	link.tunnelsWaiting = map[string]chan Frame{}
	link.pipes = map[string]io.Writer{}
	link.tunnelLock = sync.Mutex{}
	link.stream = conn
	return &link
}

// Sends down a tunnel request and creates a channel that will receive the response frame
// the caller should wait on the frame that will come as a response
// which will either be a DialResponse or an ErrorFrame
func (link *Link) Tunnel(serviceKey string) chan Frame {
	// Create new ID
	ID := NewID()
	channel := make(chan Frame, 1)

	// Create the channel to listen to the tunnel response
	link.tunnelLock.Lock()
	link.tunnelsWaiting[ID] = channel
	link.tunnelLock.Unlock()

	glog.V(2).Infof("Tunneling to %s for service %s (%s)", link.ReceiverID, serviceKey, ID)

	// Send the tunnel request
	req := &TunnelRequest{ID, serviceKey}
	_, err := link.stream.SendFrame(req)
	if err != nil {
		// Wrap error
		msg := fmt.Sprintf("Unable to send dial through link: %v", err)
		glog.Errorf("Tunneling error: %v", err)
		channel <- &ErrorFrame{msg}
	}

	return channel
}

// Loops forever or until the connection gets closed
// Handles every frame that comes through the link
func (link *Link) Maintain() {
	for {
		f, err := link.stream.GetFrame()
		if err != nil && err != io.EOF {
			glog.Warningf("Failed to get frame: %v", err)
			continue
		} else if err == io.EOF {
			glog.Errorf("Link permanently closed: EOF")
			DefaultConnectionManager.RemoveLink(link.ReceiverID)
			return
		}

		// Handle this frame
		err = link.handleFrame(f)
		if err != nil {
			glog.Warningf("Failed to handle frame: %v", err)
			continue
		}
	}
}

// Handles the data frame
// pipes that out to a listening connection
func (link *Link) handleDataFrame(data *DataFrame) error {
	glog.V(3).Infof("Link got data frame: %s", data.String())
	err := link.PipeOut(data.channelID, data.content)
	if err != nil {
		return err
	}

	glog.V(3).Infof("Successfully handled data frame (%s)", data.channelID)
	return nil
}

func (link *Link) handleTunnelRequest(req *TunnelRequest) error {
	glog.V(3).Infof("Link got tunnel request: %s", req.String())
	serviceHost, found, err := DefaultServiceResolver.GetService(req.serviceKey)
	if err != nil {
		_, err := link.stream.SendFrame(&TunnelErrorFrame{req.channelID, err.Error()})
		return err
	}

	if !found {
		_, err := link.stream.SendFrame(&TunnelErrorFrame{req.channelID, "Service not found"})
		return err
	}

	// Dial that service
	conn, err := net.Dial("tcp", serviceHost)
	if err != nil {
		glog.Errorf("Failed to dial service %s (%s): %v", req.serviceKey, req.channelID, err)
		_, err := link.stream.SendFrame(&TunnelErrorFrame{req.channelID, "Service connection error: " + err.Error()})
		return err
	}

	// Pipe all data frames coming from the channelID into that connection
	link.CreatePipe(req.channelID, conn)
	link.PipeIn(req.channelID, conn)

	// Create success response
	res := &TunnelResponse{}
	res.channelID = req.channelID

	// Done
	glog.V(2).Infof("Successfully handled tunnel request (%s)", req.channelID)
	_, err = link.stream.SendFrame(res)
	return err
}

func (link *Link) handleTunnelResponse(res *TunnelResponse) error {
	glog.V(2).Infof("Link got tunnel response: %s", res.String())

	// Find the channel that is waiting for a dial response
	channel, found := link.tunnelsWaiting[res.channelID]
	if !found {
		glog.Warningf("Tunnel response was found no associated waiting channel: %s", res.channelID)
		return nil
	}

	// Send the dial response through
	frame := &DialResponse{res.channelID}
	channel <- frame

	// Done
	glog.V(2).Infof("Successfully handled tunnel response (%s)", res.channelID)
	return nil
}

func (link *Link) handleTunnelError(res *TunnelErrorFrame) error {
	glog.V(2).Infof("Link got tunnel error: %s", res.String())

	// Find the channel that is waiting for a tunnel response
	channel, found := link.tunnelsWaiting[res.channelID]
	if !found {
		glog.Warningf("Tunnel response was found no associated waiting channel: %s", res.channelID)
		return nil
	}

	// Send the error back to the dialer
	msg := fmt.Sprintf("Tunnel error: %v", res.message)
	frame := &ErrorFrame{msg}
	channel <- frame

	// Done
	return nil
}

// All data frames with this channelID going through the link
// will be forwarded to this writer
func (link *Link) CreatePipe(channelID string, conn io.Writer) {
	glog.V(2).Infof("Link creating pipe (%s)", channelID)
	link.tunnelLock.Lock()
	defer link.tunnelLock.Unlock()
	link.pipes[channelID] = conn
}

// Copies data from the reader through the link via DataFrames
// with the channelID provided
func (link *Link) PipeIn(channelID string, conn io.Reader) {
	go func() {
		stream := NewLinkWriter(link.stream, link.ReceiverID, channelID)
		n, err := io.Copy(stream, conn)
		if err != nil && err == io.EOF {
			glog.Warningf("Pipe closed (%s). Wrote %d bytes", channelID, n)
			link.tunnelLock.Lock()
			delete(link.pipes, channelID)
			link.tunnelLock.Unlock()
			return
		} else if err != nil {
			glog.Warningf("Pipe error (%s): %v", channelID, err)
			return
		}
	}()
}

// Sends the escaped content (coming from a DataFrame) through a
// connection that corresponds to a channelID
func (link *Link) PipeOut(channelID string, content string) error {
	conn, found := link.pipes[channelID]
	if !found {
		glog.Errorf("Failed PipeOut: pipe not found: %s", channelID)
		return fmt.Errorf("Pipe not found: %s", channelID)
	}
	_, err := conn.Write(UnescapeContent(content))
	return err
}

func (link *Link) handleFrame(f Frame) error {
	switch f.Header() {
	case HEADER_DATA:
		data, ok := f.(*DataFrame)
		if !ok {
			return ImpossibleError()
		}
		return link.handleDataFrame(data)

	case HEADER_TUNNEL_REQ:
		req, ok := f.(*TunnelRequest)
		if !ok {
			return ImpossibleError()
		}
		return link.handleTunnelRequest(req)

	case HEADER_TUNNEL_RES:
		res, ok := f.(*TunnelResponse)
		if !ok {
			return ImpossibleError()
		}
		return link.handleTunnelResponse(res)

	case HEADER_TUNNEL_ERROR:
		res, ok := f.(*TunnelErrorFrame)
		if !ok {
			return ImpossibleError()
		}
		return link.handleTunnelError(res)

	case HEADER_HEARTBEAT:
		glog.V(4).Infof("Got heartbeat for %s", link.ReceiverID)
		link.LastHeartbeat = time.Now()
		return nil
	}

	return fmt.Errorf("Unrecognized header: %d", f.Header())
}
