package operator

import (
	"fmt"
	"io"
	"net"
	"time"

	"github.com/golang/glog"
)

type Link struct {
	LastHeartbeat  time.Time
	TunnelsWaiting map[string]chan string
	Pipes          map[string]net.Conn
	ReceiverID     string
	net.Conn
}

func NewLink(conn net.Conn, receiverID string) *Link {
	l := Link{time.Now(), map[string]chan string{}, map[string]net.Conn{}, receiverID, conn}
	return &l
}

func (l *Link) Tunnel(serviceKey string) chan string {
	ID := NewID()
	channel := make(chan string, 1)
	l.TunnelsWaiting[ID] = channel

	req := &TunnelRequest{ID, serviceKey}
	err := SendFrame(l, req)
	if err != nil {
		channel <- TUNNEL_ERR
	}

	return channel
}

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

func (l *Link) handleFrame(f Frame) error {
	switch f.Header() {
	case HEADER_DATA:
		req, ok := f.(*DataFrame)
		if !ok {
			return ImpossibleError()
		}

		glog.V(3).Infof("Link got data frame: %s", req.String())
		err := l.PipeOut(req.channelID, req.content)
		if err != nil {
			return err
		}

		glog.V(2).Infof("Successfully handled data frame (%s)", req.channelID)
		return nil

	case HEADER_TUNNEL_REQ:
		req, ok := f.(*TunnelRequest)
		if !ok {
			return ImpossibleError()
		}

		glog.V(3).Infof("Link got tunnel request: %s", req.String())
		serviceHost, found := DefaultConnectionManager.GetService(req.serviceKey)
		if !found {
			return SendFrame(l, &TunnelErrorFrame{req.channelID, "Service not found"})
		}

		conn, err := net.Dial("tcp", serviceHost)
		if err != nil {
			glog.Errorf("Failed to dial service %s (%s): %v", req.serviceKey, req.channelID, err)
			return SendFrame(l, &TunnelErrorFrame{req.channelID, "Service not found"})
		}

		l.CreatePipe(req.channelID, conn)
		l.PipeIn(req.channelID, conn)

		// Create response
		res := &TunnelResponse{}
		res.channelID = req.channelID

		glog.V(2).Infof("Successfully handled tunnel request (%s)", req.channelID)
		return SendFrame(l, res)

	case HEADER_TUNNEL_RES:
		res, ok := f.(*TunnelResponse)
		if !ok {
			return ImpossibleError()
		}

		glog.V(2).Infof("Link got tunnel response: %s", res.String())
		channel, found := l.TunnelsWaiting[res.channelID]
		if !found {
			glog.Warningf("Tunnel response was found no associated waiting channel: %s", res.channelID)
			return nil
		}

		channel <- res.channelID
		glog.V(2).Infof("Successfully handled tunnel response (%s)", res.channelID)
		return nil

	case HEADER_HEARTBEAT:
		glog.V(3).Infof("Link got heartbeat: %s", f.String())
		l.LastHeartbeat = time.Now()
		return nil

	case HEADER_TUNNEL_ERROR:
		res, ok := f.(*TunnelErrorFrame)
		if !ok {
			return ImpossibleError()
		}
		glog.V(2).Infof("Link got tunnel error: %s", res.String())

		channel, found := l.TunnelsWaiting[res.channelID]
		if !found {
			glog.Warningf("Tunnel response was found no associated waiting channel: %s", res.channelID)
			return nil
		}
		channel <- TUNNEL_ERR
		return nil
	}

	return fmt.Errorf("Unrecognized header: %d", f.Header())
}

// All data frames with this channelID going through the link
// will be forwarded to this connection
func (l *Link) CreatePipe(channelID string, conn net.Conn) {
	glog.V(2).Infof("Link creating pipe (%s)", channelID)
	l.Pipes[channelID] = conn
}

func (l *Link) PipeIn(channelID string, conn net.Conn) {
	buf := make([]byte, 4096)
	frame := &DataFrame{}
	go func() {
		defer conn.Close()
		for {
			n, err := io.ReadAtLeast(conn, buf, 1)
			if err == io.EOF {
				glog.Warningf("Pipe closed (%s)", channelID)
				l.Pipes[channelID] = nil
				return
			} else if err != nil {
				glog.Warningf("Pipe error (%s): %v", channelID, err)
				return
			}
			glog.V(3).Infof("Read %d bytes from pipe", n)
			frame.receiverID = l.ReceiverID
			frame.channelID = channelID
			frame.content = EscapeContent(buf[:n])

			err = SendFrame(l, frame)
			if err != nil {
				glog.Errorf("Failed to PipeIn: %v", err)
				return
			}
		}
	}()
}

func (l *Link) PipeOut(channelID string, content string) error {
	conn, found := l.Pipes[channelID]
	if !found {
		glog.Errorf("Failed PipeOut: pipe not found: %s", channelID)
		return fmt.Errorf("Pipe not found: %s", channelID)
	}
	_, err := conn.Write(UnescapeContent(content))
	return err
}
