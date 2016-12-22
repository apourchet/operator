package operator

import (
	"fmt"
	"io"
	"math/rand"
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

// Handles TunnelReq, TunnerRes, HB and Data
func (l *Link) Maintain() {
	for {
		f, err := GetFrame(l)
		if err != nil && err != io.EOF {
			glog.Warningf("Failed to get frame: %v", err)
			continue
		} else if err == io.EOF {
			glog.Errorf("Link permanently closed: EOF")
			return // TODO remove it from the list of links
		}

		glog.V(3).Infof("Got header: %d", f.Header())
		glog.V(3).Infof("Got content: %s", string(f.Content()))

		// Handle this frame
		err = l.handleFrame(f)
		if err != nil {
			glog.Warningf("Failed to handle frame: %v", err)
			continue
		}
		glog.V(3).Infof("Successfully handled frame")
	}
}

func (l *Link) handleFrame(f Frame) error {
	switch f.Header() {
	case HEADER_DATA:
		req, ok := f.(*DataFrame)
		if !ok || req.IsError() {
			// Should never happen
			// TODO log
			return nil
		}
		glog.V(3).Infof("Link got data frame: %s", req.String())
		err := l.PipeOut(req.channelID, req.content)
		if err != nil {
			return err
		}
		glog.V(3).Infof("Data frame piped successfully")
		return nil

	case HEADER_TUNNEL_REQ:
		req, ok := f.(*TunnelRequest)
		res := &TunnelResponse{}
		if !ok || req.IsError() {
			// Should never happen
			// TODO Log
			return nil
		}
		glog.V(2).Infof("Link got tunnel request: %s", req.String())
		serviceHost, found := DefaultConnectionManager.GetService(req.serviceKey)
		if !found {
			return fmt.Errorf("ServiceKey not found: %s", req.serviceKey)
		}

		conn, err := net.Dial("tcp", serviceHost)
		if err != nil {
			glog.Errorf("Failed to create pipe: %v", err)
			return err
		}

		l.CreatePipe(req.channelID, conn)
		l.PipeIn(req.channelID, conn)

		res.channelID = req.channelID
		return SendFrame(l, res)

	case HEADER_TUNNEL_RES:
		res, ok := f.(*TunnelResponse)
		if !ok || res.IsError() {
			// Should never happen
			// TODO log
			return fmt.Errorf("Failed to cast tunnel response")
		}
		glog.V(2).Infof("Link got tunnel response: %s", res.String())
		if channel, found := l.TunnelsWaiting[res.channelID]; found {
			channel <- res.channelID
		} else {
			glog.Warningf("Tunnel response was found no associated waiting channel: %s", res.channelID)
		}
		return nil

	case HEADER_HEARTBEAT:
		l.LastHeartbeat = time.Now()
		return nil
	}
	return fmt.Errorf("Unrecognized header: %d", f.Header())
}

// All data frames with this channelID going through the link
// will be forwarded to this connection
func (l *Link) CreatePipe(channelID string, conn net.Conn) error {
	glog.V(2).Infof("Link creating pipe (%s)", channelID)
	l.Pipes[channelID] = conn
	return nil
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
				glog.Infof("Failed to PipeIn: %v", err)
				return
			}
		}
	}()
}

func (l *Link) PipeOut(channelID string, content string) error {
	conn, found := l.Pipes[channelID]
	if !found {
		glog.Errorf("Pipe not found: %s", channelID)
		return fmt.Errorf("Pipe not found: %s", channelID)
	}
	_, err := conn.Write(UnescapeContent(content))
	return err
}

func NewID() string {
	rand.Seed(time.Now().UTC().UnixNano())
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	result := make([]byte, 10)
	for i := 0; i < 10; i++ {
		result[i] = chars[rand.Intn(len(chars))]
	}
	return string(result)
}
