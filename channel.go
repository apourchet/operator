package operator

import (
	"encoding/base64"
	"net"
	"time"

	"github.com/golang/glog"
)

type Channel struct {
	conn       net.Conn
	receiverID string
	channelID  string
}

func NewChannel(conn net.Conn, receiverID string, channelID string) net.Conn {
	c := Channel{conn, receiverID, channelID}
	glog.V(3).Infof("New channel: %s (%s)", c.receiverID, c.channelID)
	return &c
}

func (c *Channel) Read(b []byte) (n int, err error) {
	glog.V(3).Infof("Channel %s reading", c.receiverID)
	return c.conn.Read(b)
}

func (c *Channel) Write(b []byte) (n int, err error) {
	glog.V(3).Infof("Channel %s writing: %s", c.receiverID, string(b))
	f := &DataFrame{}
	f.receiverID = c.receiverID
	f.channelID = c.channelID
	f.content = EscapeContent(b)
	// TODO do this properly
	return len(b), SendFrame(c.conn, f)
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

func EscapeContent(content []byte) string {
	enc := base64.StdEncoding.EncodeToString(content)
	return enc
}

func UnescapeContent(content string) []byte {
	dec, _ := base64.StdEncoding.DecodeString(content)
	return dec
}
