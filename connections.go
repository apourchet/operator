package operator

import "net"

type ConnectionManager interface {
	SetLink(receiverID string, conn net.Conn)
	GetLink(receiverID string) *Link
	RemoveLink(receiverID string)

	SetService(serviceName string, host string)
	GetService(serviceName string) (string, bool)
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
	l := NewLink(conn, receiverID)
	c.Links[receiverID] = l
	go l.Maintain()
}

func (c *connectionManager) GetLink(receiverID string) *Link {
	if l, found := c.Links[receiverID]; found {
		return l
	}
	return nil
}

func (c *connectionManager) RemoveLink(receiverID string) {
	delete(c.Links, receiverID)
}

func (c *connectionManager) SetService(serviceName string, host string) {
	c.Services[serviceName] = host
}

func (c *connectionManager) GetService(serviceName string) (string, bool) {
	serviceHost, found := c.Services[serviceName]
	return serviceHost, found
}
