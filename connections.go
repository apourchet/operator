package operator

import (
	"fmt"
	"net"
)

type ConnectionManager interface {
	SetLink(receiverID string, conn net.Conn) error
	GetLink(receiverID string) (*Link, error)
	RemoveLink(receiverID string) error

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

func (c *connectionManager) SetLink(receiverID string, conn net.Conn) error {
	l := NewLink(conn, receiverID)
	c.Links[receiverID] = l
	go l.Maintain()
	return nil
}

func (c *connectionManager) GetLink(receiverID string) (*Link, error) {
	if l, found := c.Links[receiverID]; found {
		return l, nil
	}
	return nil, fmt.Errorf("Link not found")
}

func (c *connectionManager) RemoveLink(receiverID string) error {
	delete(c.Links, receiverID)
	return nil
}

func (c *connectionManager) SetService(serviceName string, host string) {
	c.Services[serviceName] = host
}

func (c *connectionManager) GetService(serviceName string) (string, bool) {
	serviceHost, found := c.Services[serviceName]
	return serviceHost, found
}
