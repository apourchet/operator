package operator

import (
	"fmt"
	"io"
	"net"
	"time"

	"github.com/golang/glog"
)

type Link struct {
	LastHeartbeat time.Time
	net.Conn
}

func NewLink(conn net.Conn) *Link {
	l := Link{time.Now(), conn}
	return &l
}

func (l *Link) Tunnel(serviceName string) chan string {
	// TODO
	c := make(chan string, 1)
	c <- "TESTID"
	return c
}

// Handles TunnelReq, TunnerRes, HB and Data
func (l *Link) Maintain() {
	for {
		h, content, err := getFrame(l)
		if err != nil && err != io.EOF {
			glog.Warningf("Failed to get frame: %v", err)
			continue
		} else if err == io.EOF {
			glog.Errorf("Link permanently closed: EOF")
			return
		}

		glog.V(3).Infof("Got header: %d", h)
		glog.V(3).Infof("Got content: %s", content)

		// Handle this frame
		err = l.handleFrame(h, content)
		if err != nil {
			glog.Warningf("Failed to handle frame: %v", err)
			continue
		}
		glog.V(3).Infof("Successfully handled frame")
	}
}

func (l *Link) handleFrame(h byte, content string) error {
	switch h {
	case HEADER_DATA:
		break
	case HEADER_TUNNEL_REQ:
		break
	case HEADER_TUNNEL_RES:
		break
	case HEADER_HEARTBEAT:
		l.LastHeartbeat = time.Now()
		return nil
	}
	return fmt.Errorf("Unrecognized header: %d", h)
}
