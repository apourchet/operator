package operator

import (
	"bufio"
	"bytes"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Fatalize(t *testing.T, err error) {
	if err != nil {
		debug.PrintStack()
		t.Fatalf("Error should be nil: %v", err)
	}
}

func TestOne(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})

	frame := &HeartbeatFrame{}
	n, err := sendFrame(buf, frame)
	Fatalize(t, err)
	assert.NotEqual(t, 0, n)

	frame1, err := getFrame(bufio.NewReader(buf))
	Fatalize(t, err)
	assert.Equal(t, frame, frame1)
}

func TestTwo(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	reader := bufio.NewReader(buf)

	frame := &HeartbeatFrame{}

	// Send 1st frame
	n, err := sendFrame(buf, frame)
	Fatalize(t, err)
	assert.NotEqual(t, 0, n)

	// Send 2nd frame
	n, err = sendFrame(buf, frame)
	Fatalize(t, err)
	assert.NotEqual(t, 0, n)

	// Get 1st frame
	frame1, err := getFrame(reader)
	Fatalize(t, err)
	assert.Equal(t, frame, frame1)

	// Get 2nd frame
	frame1, err = getFrame(reader)
	Fatalize(t, err)
	assert.Equal(t, frame, frame1)
}
