package operator

import (
	"net"
	"sync"
)

type LinkWriter struct {
	dest  net.Conn
	frame *DataFrame
	lock  sync.Mutex
}

func NewLinkWriter(dest net.Conn, receiverID, channelID string) *LinkWriter {
	return &LinkWriter{dest, &DataFrame{receiverID, channelID, ""}, sync.Mutex{}}
}

func (l *LinkWriter) Write(p []byte) (int, error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.frame.content = EscapeContent(p)
	_, err := SendFrame(l.dest, l.frame)
	return len(p), err
}
