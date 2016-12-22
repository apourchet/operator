package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/apourchet/operator"
	"github.com/golang/glog"
)

func init() {
	flag.Set("logtostderr", "true")
}

func main() {
	flag.Parse()

	tr := &http.Transport{
		DialContext: operator.DialContext("phone1", "key1"),
	}

	client := &http.Client{Transport: tr}
	resp, err := client.Get("http://localhost:10000/foo")
	if err != nil {
		glog.Fatal(err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Fatal(err)
	}
	fmt.Println("Response: ", string(body))
}
