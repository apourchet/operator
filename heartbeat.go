package operator

import (
	"net"
	"time"

	"github.com/golang/glog"
)

type HeartbeatManager interface {
	// Gets the interval in between the heartbeats
	GetInterval() time.Duration
}

var DefaultHeartbeatManager HeartbeatManager = &heartbeatManager{}

type heartbeatManager struct{}

// Sends heartbeats as long as the connection isn't closed
// and we do not get an error
func SendHeartbeats(conn net.Conn) error {
	hb := &HeartbeatFrame{}
	for {
		_, err := SendFrame(conn, hb)
		if err != nil {
			glog.Warningf("Failed to heartbeat: %v", err)
			return err
		}
		time.Sleep(DefaultHeartbeatManager.GetInterval())
	}
}

func (hm *heartbeatManager) GetInterval() time.Duration {
	return 2 * time.Second
}
