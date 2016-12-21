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
	net.Conn
}

func NewLink(conn net.Conn) *Link {
	l := Link{time.Now(), map[string]chan string{}, conn}
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
		// TODO
		return nil
	case HEADER_TUNNEL_REQ:
		// TODO
		// Look in CM to find service split[0]
		// Create TCP to split[0]
		// Create pipe from link to that connection
		// Send OK response
		return nil
	case HEADER_TUNNEL_RES:
		res, ok := f.(*TunnelResponse)
		if !ok || res.IsError() {
			// Should never happen
			return fmt.Errorf("Failed to cast tunnel response")
		}
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

// TODO Randomize
func NewID() string {
	return "TESTID"
}
