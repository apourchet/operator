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

	dialer := operator.NewDialer(nil)
	dialer.OperatorResolver.SetOperator("phone1", "localhost:10000")
	tr := &http.Transport{DialContext: dialer.DialContext()}

	for i := 0; i < 10; i++ {
		client := &http.Client{Transport: tr}
		resp, err := client.Get("http://phone1.key1/foo")
		if err != nil {
			glog.Fatal(err)
		}
		defer resp.Body.Close()

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			glog.Fatal(err)
		}
		if string(body) != "bar" {
			fmt.Println("ERROR", string(body))
		} else {
			fmt.Println("Success", i)
		}
	}
}
