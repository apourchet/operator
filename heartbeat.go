package operator

import (
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
func SendHeartbeats(conn FrameWriter) error {
	hb := &HeartbeatFrame{}
	for {
		_, err := conn.SendFrame(hb)
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
