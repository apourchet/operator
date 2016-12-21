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

	operator.DefaultOperator.SetID("phone1")
	// Link from private network to internal servers
	err := operator.LinkAndServe(10001, "localhost:10000")
	if err != nil {
		glog.Fatal(err)
	}
}
