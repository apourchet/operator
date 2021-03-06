package operator

import (
	"bufio"
	"fmt"
	"io"
	"strings"

	"github.com/golang/glog"
)

// The header type will be contained in the first byte
// of each line.
const (
	HEADER_ERROR        = 0
	HEADER_TUNNEL_ERROR = 1

	HEADER_DATA         = '0'
	HEADER_LINK_REQ     = '1'
	HEADER_LINK_RES     = '2'
	HEADER_REGISTER_REQ = '3'
	HEADER_REGISTER_RES = '4'
	HEADER_DIAL_REQ     = '5'
	HEADER_DIAL_RES     = '6'
	HEADER_TUNNEL_REQ   = '7'
	HEADER_TUNNEL_RES   = '8'
	HEADER_HEARTBEAT    = '9'
)

// Frame interface
type Frame interface {
	Header() byte
	Content() []byte
	String() string // For debugging
	IsError() bool
	Parse(string) error
}

type ErrorFrame struct {
	message string
}

type TunnelErrorFrame struct {
	channelID string
	message   string
}

type DataFrame struct {
	receiverID string
	channelID  string
	content    string
}

type LinkRequest struct {
	receiverID string
}
type LinkResponse struct {
	receiverID string
}

type RegisterRequest struct {
	serviceHost string
	serviceKey  string
}
type RegisterResponse struct{}

type DialRequest struct {
	receiverID string
	serviceKey string
}
type DialResponse struct {
	channelID string
}

type TunnelRequest struct {
	channelID  string
	serviceKey string
}
type TunnelResponse struct {
	channelID string
}

type HeartbeatFrame struct{}

// ErrorFrame
func (f *ErrorFrame) Header() byte { return HEADER_ERROR }
func (f *ErrorFrame) Content() []byte {
	return []byte(f.message)
}
func (f *ErrorFrame) String() string { return fmt.Sprintf("%#v", f) }
func (f *ErrorFrame) IsError() bool  { return true }

func (f *ErrorFrame) Parse(content string) error {
	f.message = content
	return nil
}

// TunnelErrorFrame
func (f *TunnelErrorFrame) Header() byte { return HEADER_TUNNEL_ERROR }
func (f *TunnelErrorFrame) Content() []byte {
	return []byte(f.channelID + "," + f.message)
}
func (f *TunnelErrorFrame) String() string { return fmt.Sprintf("%#v", f) }
func (f *TunnelErrorFrame) IsError() bool  { return true }

func (f *TunnelErrorFrame) Parse(content string) error {
	split := strings.Split(content, ",")
	if len(split) != 2 {
		return fmt.Errorf("TunnelErrorFrame parse error: '%s'", content)
	}
	f.channelID = split[0]
	f.message = split[1]
	return nil
}

// DataFrame
func (f *DataFrame) Header() byte { return HEADER_DATA }
func (f *DataFrame) Content() []byte {
	return []byte(f.receiverID + "," + f.channelID + "," + f.content)
}
func (f *DataFrame) String() string { return fmt.Sprintf("%#v", f) }
func (f *DataFrame) IsError() bool  { return false }

func (f *DataFrame) Parse(content string) error {
	split := strings.Split(content, ",")
	if len(split) != 3 {
		return fmt.Errorf("DataFrame parse error: '%s'", content)
	}
	f.receiverID = split[0]
	f.channelID = split[1]
	f.content = split[2]
	return nil
}

// LinkRequest
func (f *LinkRequest) Header() byte { return HEADER_LINK_REQ }
func (f *LinkRequest) Content() []byte {
	return []byte(f.receiverID)
}
func (f *LinkRequest) String() string { return fmt.Sprintf("%#v", f) }
func (f *LinkRequest) IsError() bool  { return false }

func (f *LinkRequest) Parse(content string) error {
	f.receiverID = content
	return nil
}

// LinkResponse
func (f *LinkResponse) Header() byte { return HEADER_LINK_RES }
func (f *LinkResponse) Content() []byte {
	return []byte(f.receiverID)
}
func (f *LinkResponse) String() string { return fmt.Sprintf("%#v", f) }
func (f *LinkResponse) IsError() bool  { return false }

func (f *LinkResponse) Parse(content string) error {
	f.receiverID = content
	return nil
}

// RegisterRequest
func (f *RegisterRequest) Header() byte { return HEADER_REGISTER_REQ }
func (f *RegisterRequest) Content() []byte {
	return []byte(f.serviceHost + "," + f.serviceKey)
}
func (f *RegisterRequest) String() string { return fmt.Sprintf("%#v", f) }
func (f *RegisterRequest) IsError() bool  { return false }

func (f *RegisterRequest) Parse(content string) error {
	split := strings.Split(content, ",")
	if len(split) != 2 {
		return fmt.Errorf("RegisterRequest parse error: '%s'", content)
	}
	f.serviceHost = split[0]
	f.serviceKey = split[1]
	return nil
}

// RegisterResponse
func (f *RegisterResponse) Header() byte    { return HEADER_REGISTER_RES }
func (f *RegisterResponse) Content() []byte { return []byte{} }
func (f *RegisterResponse) String() string  { return fmt.Sprintf("%#v", f) }
func (f *RegisterResponse) IsError() bool   { return false }

func (f *RegisterResponse) Parse(content string) error {
	if content != "" {
		return fmt.Errorf("RegisterResponse should be empty")
	}
	return nil
}

// DialRequest
func (f *DialRequest) Header() byte { return HEADER_DIAL_REQ }
func (f *DialRequest) Content() []byte {
	return []byte(f.receiverID + "," + f.serviceKey)
}
func (f *DialRequest) String() string { return fmt.Sprintf("%#v", f) }
func (f *DialRequest) IsError() bool  { return false }

func (f *DialRequest) Parse(content string) error {
	split := strings.Split(content, ",")
	if len(split) != 2 {
		return fmt.Errorf("DialRequest parse error: '%s'", content)
	}
	f.receiverID = split[0]
	f.serviceKey = split[1]
	return nil
}

// DialResponse
func (f *DialResponse) Header() byte { return HEADER_DIAL_RES }
func (f *DialResponse) Content() []byte {
	return []byte(f.channelID)
}
func (f *DialResponse) String() string { return fmt.Sprintf("%#v", f) }
func (f *DialResponse) IsError() bool  { return false }

func (f *DialResponse) Parse(content string) error {
	f.channelID = content
	return nil
}

// TunnelRequest
func (f *TunnelRequest) Header() byte { return HEADER_TUNNEL_REQ }
func (f *TunnelRequest) Content() []byte {
	return []byte(f.channelID + "," + f.serviceKey)
}
func (f *TunnelRequest) String() string { return fmt.Sprintf("%#v", f) }
func (f *TunnelRequest) IsError() bool  { return false }

func (f *TunnelRequest) Parse(content string) error {
	split := strings.Split(content, ",")
	if len(split) != 2 {
		return fmt.Errorf("TunnelRequest parse error: '%s'", content)
	}
	f.channelID = split[0]
	f.serviceKey = split[1]
	return nil
}

// TunnelResponse
func (f *TunnelResponse) Header() byte { return HEADER_TUNNEL_RES }
func (f *TunnelResponse) Content() []byte {
	return []byte(f.channelID)
}
func (f *TunnelResponse) String() string { return fmt.Sprintf("%#v", f) }
func (f *TunnelResponse) IsError() bool  { return false }

func (f *TunnelResponse) Parse(content string) error {
	f.channelID = content
	return nil
}

// Heartbeat
func (f *HeartbeatFrame) Header() byte    { return HEADER_HEARTBEAT }
func (f *HeartbeatFrame) Content() []byte { return []byte{} }
func (f *HeartbeatFrame) String() string  { return fmt.Sprintf("%#v", f) }
func (f *HeartbeatFrame) IsError() bool   { return false }

func (f *HeartbeatFrame) Parse(content string) error {
	if content != "" {
		return fmt.Errorf("Heartbeat should be empty. Instead: '%s'", content)
	}
	return nil
}

const (
	FRAME_DELIMITER = '\n'
)

func sendFrame(conn io.Writer, frame Frame) (int, error) {
	// glog.V(3).Infof("Sending frame: %s", frame.String())
	data := append([]byte{frame.Header()}, frame.Content()...)
	data = append(data, FRAME_DELIMITER)
	// glog.V(3).Infof("Sending through conn: %v", data)
	return conn.Write(data)
}

func getFrame(reader *bufio.Reader) (Frame, error) {
	// Read the content that ends with a newline character
	content, err := reader.ReadString(FRAME_DELIMITER)
	if err != nil {
		return nil, err
	}
	h := content[0]
	content = content[1 : len(content)-1]

	switch h {
	case HEADER_ERROR:
		f := &ErrorFrame{}
		return f, f.Parse(content)
	case HEADER_TUNNEL_ERROR:
		f := &TunnelErrorFrame{}
		return f, f.Parse(content)
	case HEADER_DATA:
		f := &DataFrame{}
		return f, f.Parse(content)
	case HEADER_LINK_REQ:
		f := &LinkRequest{}
		return f, f.Parse(content)
	case HEADER_LINK_RES:
		f := &LinkResponse{}
		return f, f.Parse(content)
	case HEADER_REGISTER_REQ:
		f := &RegisterRequest{}
		return f, f.Parse(content)
	case HEADER_REGISTER_RES:
		f := &RegisterResponse{}
		return f, f.Parse(content)
	case HEADER_DIAL_REQ:
		f := &DialRequest{}
		return f, f.Parse(content)
	case HEADER_DIAL_RES:
		f := &DialResponse{}
		return f, f.Parse(content)
	case HEADER_TUNNEL_REQ:
		f := &TunnelRequest{}
		return f, f.Parse(content)
	case HEADER_TUNNEL_RES:
		f := &TunnelResponse{}
		return f, f.Parse(content)
	case HEADER_HEARTBEAT:
		f := &HeartbeatFrame{}
		return f, f.Parse(content)
	}

	glog.Errorf("Unrecognized header: %x => %s", h, content)
	return nil, fmt.Errorf("Unrecognized header: %x", h)
}
