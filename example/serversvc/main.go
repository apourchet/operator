package main

import (
	"flag"

	"github.com/apourchet/operator"
	"github.com/golang/glog"
)

func init() {
	flag.Set("logtostderr", "true")
}

func main() {
	flag.Parse()

	conn, err := operator.Dial("localhost:10000", "phone1", "key1")
	if err != nil {
		glog.Fatal(err)
	}

	conn.Write([]byte("GET /foo HTTP/1.0\r\n\r\n"))

	select {}
}
