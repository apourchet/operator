package operator

import (
	"encoding/base64"
	"fmt"
	"math/rand"
	"time"

	"github.com/golang/glog"
)

func NewID() string {
	rand.Seed(time.Now().UTC().UnixNano())
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	result := make([]byte, 10)
	for i := 0; i < 10; i++ {
		result[i] = chars[rand.Intn(len(chars))]
	}
	return string(result)
}

func EscapeContent(content []byte) string {
	enc := base64.StdEncoding.EncodeToString(content)
	return enc
}

func UnescapeContent(content string) []byte {
	dec, _ := base64.StdEncoding.DecodeString(content)
	return dec
}

func ImpossibleError() error {
	glog.Warningf("An impossible error happened...")
	return fmt.Errorf("Impossible error occured")
}
