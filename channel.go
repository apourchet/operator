package operator

import (
	"fmt"
	"net"
	"time"

	"github.com/golang/glog"
)

// net.Conn that will prepend all writes with HEADER_DATA and the channelID
type Channel struct {
	conn net.Conn
	ID   string
}

func NewChannel(conn net.Conn, ID string) net.Conn {
	c := Channel{conn, ID}
	glog.V(3).Infof("New channel: %s", c.ID)
	return &c
}

func (c *Channel) Read(b []byte) (n int, err error) {
	glog.V(3).Infof("Channel %s reading", c.ID)
	// TODO
	return c.conn.Read(b)
}

func (c *Channel) Write(b []byte) (n int, err error) {
	glog.V(3).Infof("Channel %s writing: %s", c.ID, string(b))
	content := []byte(fmt.Sprintf("%s,%s", c.ID, string(b)))
	content = append([]byte{HEADER_DATA}, content...)
	// TODO
	return c.conn.Write(content)
}

func (c *Channel) Close() error {
	return c.conn.Close()
}

func (c *Channel) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *Channel) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *Channel) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

func (c *Channel) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *Channel) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}
