package operator

import (
	"fmt"
	"net"
)

type ConnectionManager interface {
	SetLink(receiverID string, conn net.Conn) error
	GetLink(receiverID string) (*Link, error)
	RemoveLink(receiverID string) error
}

var DefaultConnectionManager ConnectionManager = newConnectionManager()

type connectionManager struct {
	Links map[string]*Link
}

func newConnectionManager() ConnectionManager {
	return &connectionManager{map[string]*Link{}}
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
