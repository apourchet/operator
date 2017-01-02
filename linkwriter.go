package operator

type LinkWriter struct {
	dest       FrameWriter
	receiverID string
	channelID  string
}

func NewLinkWriter(dest FrameWriter, receiverID, channelID string) *LinkWriter {
	return &LinkWriter{dest, receiverID, channelID}
}

func (lw *LinkWriter) Write(p []byte) (int, error) {
	frame := &DataFrame{lw.receiverID, lw.channelID, EscapeContent(p)}
	_, err := lw.dest.SendFrame(frame)
	return len(p), err
}
