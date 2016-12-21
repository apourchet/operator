package main

import (
	"flag"
	"fmt"
	"net/http"

	"github.com/apourchet/operator"
	"github.com/golang/glog"
)

func init() {
	flag.Set("logtostderr", "true")
}

func main() {
	flag.Parse()

	// Register listener to server
	err := operator.RegisterListener("localhost:10002", "localhost:10001", "key1")
	if err != nil {
		glog.Fatal(err)
	}

	// Start that service
	http.HandleFunc("/foo", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "bar")
	})
	err = http.ListenAndServe(":10002", nil)
	if err != nil {
		glog.Fatal(err)
	}
}