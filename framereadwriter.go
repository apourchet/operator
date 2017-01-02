package operator

import (
	"bufio"
	"io"
)

type FrameReader interface {
	GetFrame() (Frame, error)
}

type FrameWriter interface {
	SendFrame(Frame) (int, error)
}

type FrameReadWriter interface {
	FrameReader
	FrameWriter
	io.ReadWriter
}

type bufferedConnection struct {
	buffer *bufio.Reader
	io.ReadWriter
}

func NewBufferedConnection(rw io.ReadWriter) FrameReadWriter {
	reader := bufio.NewReader(rw)
	conn := &bufferedConnection{reader, rw}
	return conn
}

func (conn *bufferedConnection) GetFrame() (Frame, error) {
	return getFrame(conn.buffer)
}

func (conn *bufferedConnection) SendFrame(frame Frame) (int, error) {
	return sendFrame(conn, frame)
}
