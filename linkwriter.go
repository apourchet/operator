package operator

import "net"

type LinkWriter struct {
	dest  net.Conn
	frame *DataFrame
}

func NewLinkWriter(dest net.Conn, receiverID, channelID string) *LinkWriter {
	return &LinkWriter{dest, &DataFrame{receiverID, channelID, ""}}
}

func (l *LinkWriter) Write(p []byte) (int, error) {
	l.frame.content = EscapeContent(p)
	_, err := SendFrame(l.dest, l.frame)
	return len(p), err
}
