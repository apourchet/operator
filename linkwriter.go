package operator

import "net"

type LinkWriter struct {
	dest       net.Conn
	receiverID string
	channelID  string
}

func NewLinkWriter(dest net.Conn, receiverID, channelID string) *LinkWriter {
	return &LinkWriter{dest, receiverID, channelID}
}

func (l *LinkWriter) Write(p []byte) (int, error) {
	frame := &DataFrame{l.receiverID, l.channelID, EscapeContent(p)}
	_, err := SendFrame(l.dest, frame)
	return len(p), err
}
