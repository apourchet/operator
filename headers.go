package operator

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

// The header type will be contained in the first byte
// of each line.
const (
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

	DIAL_ERROR  = "ERR"
	REGISTER_OK = "OK"
)

func SendHeartbeat(conn net.Conn) error {
	content := []byte{HEADER_HEARTBEAT}
	content = append(content, '\n')
	_, err := conn.Write(content)
	return err
}

func SendLinkRequest(conn net.Conn, receiverID string) error {
	content := append([]byte{HEADER_LINK_REQ}, []byte(receiverID)...)
	content = append(content, '\n')
	_, err := conn.Write(content)
	return err
}

func SendLinkResponse(conn net.Conn, receiverID string) error {
	content := append([]byte{HEADER_LINK_RES}, []byte(receiverID)...)
	content = append(content, '\n')
	_, err := conn.Write(content)
	return err
}

func SendRegisterRequest(conn net.Conn, servicename string, channelKey string) error {
	content := []byte(fmt.Sprintf("%s,%s", servicename, channelKey))
	content = append([]byte{HEADER_REGISTER_REQ}, content...)
	content = append(content, '\n')
	_, err := conn.Write(content)
	return err
}

func SendRegisterResponse(conn net.Conn) error {
	content := append([]byte{HEADER_REGISTER_RES}, []byte(REGISTER_OK)...)
	content = append(content, '\n')
	_, err := conn.Write(content)
	return err
}

func SendDialRequest(conn net.Conn, receiverID, key string) error {
	content := []byte(fmt.Sprintf("%s,%s", receiverID, key))
	content = append([]byte{HEADER_DIAL_REQ}, content...)
	content = append(content, '\n')
	_, err := conn.Write(content)
	return err
}

func SendDialResponse(conn net.Conn, channelID string) error {
	content := append([]byte{HEADER_DIAL_RES}, []byte(channelID)...)
	content = append(content, '\n')
	_, err := conn.Write(content)
	return err
}

func SendTunnelRequest(conn net.Conn, serviceName string) error {
	content := append([]byte{HEADER_TUNNEL_REQ}, []byte(serviceName)...)
	content = append(content, '\n')
	_, err := conn.Write(content)
	return err
}

func SendTunnelResponse(conn net.Conn, serviceName string, channelID string) error {
	content := []byte(fmt.Sprintf("%s,%s", serviceName, channelID))
	content = append([]byte{HEADER_TUNNEL_RES}, content...)
	content = append(content, '\n')
	_, err := conn.Write(content)
	return err
}

func getFrame(conn net.Conn) (byte, string, error) {
	// Read the first byte to get the header
	h := make([]byte, 1)
	n, err := conn.Read(h)
	if err != nil {
		return 0, "", err
	}
	if n != 1 {
		return 0, "", fmt.Errorf("Failed to read header")
	}

	// Read the content that ends with a newline character
	content, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return 0, "", err
	}

	return h[0], strings.Trim(content, "\n "), nil
}
