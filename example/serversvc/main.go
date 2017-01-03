package main

import (
	"flag"
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
	// simple()
	// single()
	// many()
	parallel()
}

func simple() {
	dialer := operator.NewDialer(nil)
	dialer.OperatorResolver.SetOperator("phone0", "localhost:10000")
	tr := &http.Transport{DialContext: dialer.DialContext()}

	client := &http.Client{Transport: tr}
	resp, err := client.Get("http://phone0.foo/foo")
	if err != nil {
		glog.Fatal(err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Fatal(err)
	}
	glog.Infof(string(body))
}

func single() {
	dialer := operator.NewDialer(nil)
	dialer.OperatorResolver.SetOperator("phone0", "localhost:10000")
	tr := &http.Transport{DialContext: dialer.DialContext()}

	client := &http.Client{Transport: tr}
	req, err := http.NewRequest("GET", "http://phone0.godoc/", nil)
	if err != nil {
		glog.Fatal(err)
	}
	req.Header.Set("Connection", "close")

	resp, err := client.Do(req)
	if err != nil {
		glog.Fatal(err)
	}
	defer resp.Body.Close()

	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Fatal(err)
	}
	glog.Infof("Success")
}

func many() {
	for i := 0; i < 10000; i++ {
		single()
	}
}

func parallel() {
	for i := 0; i < 100; i++ {
		go single()
	}
	select {}
}
