package operator

import (
	"bytes"
	"fmt"
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
	n, err := SendFrame(buf, frame)
	Fatalize(t, err)
	assert.NotEqual(t, 0, n)

	frame1, err := GetFrame(buf)
	Fatalize(t, err)
	assert.Equal(t, frame, frame1)
}

func TestTwo(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})

	frame := &HeartbeatFrame{}
	n, err := SendFrame(buf, frame)
	Fatalize(t, err)
	assert.NotEqual(t, 0, n)
	n, err = SendFrame(buf, frame)
	Fatalize(t, err)
	assert.NotEqual(t, 0, n)

	fmt.Println(buf.Bytes())
	frame1, err := GetFrame(buf)
	Fatalize(t, err)
	assert.Equal(t, frame, frame1)

	fmt.Println(buf.Bytes())
	frame1, err = GetFrame(buf)
	Fatalize(t, err)
	assert.Equal(t, frame, frame1)
}
