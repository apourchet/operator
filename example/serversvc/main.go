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
	single()
	many()
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
	resp, err := client.Get("http://phone0.godoc/")
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
	for i := 0; i < 1000; i++ {
		single()
	}
}
