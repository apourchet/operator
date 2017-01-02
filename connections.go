package operator

import (
	"fmt"
	"sync"
)

type ConnectionManager interface {
	SetLink(receiverID string, conn FrameReadWriter) error
	GetLink(receiverID string) (*Link, error)
	RemoveLink(receiverID string) error
}

var DefaultConnectionManager ConnectionManager = newConnectionManager()

type connectionManager struct {
	Links map[string]*Link
	lock  sync.Mutex
}

func newConnectionManager() ConnectionManager {
	return &connectionManager{map[string]*Link{}, sync.Mutex{}}
}

func (c *connectionManager) SetLink(receiverID string, conn FrameReadWriter) error {
	l := NewLink(conn, receiverID)
	c.lock.Lock()
	defer c.lock.Unlock()
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
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.Links, receiverID)
	return nil
}
