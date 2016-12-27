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
	err := operator.NewOperator().SetID("server1").Serve(10000)
	if err != nil {
		glog.Fatal(err)
	}
}
