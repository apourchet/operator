package main

import (
	"flag"
	"fmt"

	"github.com/apourchet/operator"
	"github.com/golang/glog"
)

func init() {
	flag.Set("logtostderr", "true")
}

func usage() {
	fmt.Println("Usage: operator-register <local-operator> <servicename> <service-addr>")
}

func main() {
	flag.Parse()
	if len(flag.Args()) < 3 {
		usage()
		return
	}

	// Register listener to local operator on localhost:10001
	err := operator.RegisterService(flag.Args()[0], flag.Args()[1], flag.Args()[2])
	if err != nil {
		glog.Fatal(err)
	}
}
