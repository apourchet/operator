package main

import (
	"flag"
	"strconv"

	"github.com/apourchet/operator"
	"github.com/golang/glog"
)

func init() {
	flag.Set("logtostderr", "true")
}

func main() {
	flag.Parse()

	single()
}

func single() {
	err := operator.NewOperator("phone0", "localhost:10001").LinkAndServe(10001, "localhost:10000")
	if err != nil {
		glog.Fatal(err)
	}
}

func many() {
	for i := 0; i < 1900; i++ {
		istr := strconv.Itoa(i)
		port := 13010 + i
		portStr := strconv.Itoa(port)
		go func() { // Link from private network to internal servers
			err := operator.NewOperator("phone"+istr, "localhost:"+portStr).LinkAndServe(port, "localhost:10000")
			if err != nil {
				glog.Fatal(err)
			}
		}()
	}
	select {}
}
