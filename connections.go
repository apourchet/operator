package operator

import "net"

type ConnectionManager interface {
	SetLink(receiverID string, conn net.Conn)
	GetLink(receiverID string) *Link

	SetService(serviceName string, host string)
}

var DefaultConnectionManager ConnectionManager = newConnectionManager()

type connectionManager struct {
	Links    map[string]*Link
	Services map[string]string // map from service key to host
}

func newConnectionManager() ConnectionManager {
	return &connectionManager{map[string]*Link{}, map[string]string{}}
}

func (c *connectionManager) SetLink(receiverID string, conn net.Conn) {
	l := NewLink(conn)
	c.Links[receiverID] = l
	go l.Maintain()
}

func (c *connectionManager) GetLink(receiverID string) *Link {
	if l, found := c.Links[receiverID]; found {
		return l
	}
	return nil
}

func (c *connectionManager) SetService(serviceName string, host string) {
	c.Services[serviceName] = host
}