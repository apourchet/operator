# Operator
Scalable framework for using long-lasting bytestreams to run 
standard http/http2 or any other kind of protocol built on top of 
tcp.
Particularly useful for server-side push of updates to a device or 
machine that is not reachable from the external internet.

## Installation
```go
go get github.com/apourchet/operator
```

## Example walkthrough
### Reachable server
Setup an operator (or a set of operators) on your servers that will accept 
requests to create bytestreams (or links) from the machines that you want 
to give push notifications to:
```go
func main() {
	flag.Parse()
	err := operator.NewOperator("myserver1", "myserver1.example.com").Serve(10000)
	if err != nil {
		glog.Fatal(err)
	}
}
```

### Unreachable machine
These machines will "link" to the reachable servers setup previously and keep 
that tcp connection alive through application-level heartbeats:
```go
func main() {
	flag.Parse()

	// Link from private network to internal servers
	err := operator.NewOperator("unreachable1", "localhost:10001").LinkAndServe(10001, "myservers.example.com") // This should get loadbalanced to myserver1.example.com
	if err != nil {
		glog.Fatal(err)
	}
}
```

### Unreachable service
Now that the operator server is running on that unreachable machine, and that a link was
created, we can start a simple http service and register it to the local operator:
```go
func main() {
	flag.Parse()

	// Register listener to operator
	err := operator.RegisterService("localhost:10001", "my-service", "localhost:10002")
	if err != nil {
		glog.Fatal(err)
	}

	// Start that service
	http.HandleFunc("/foo", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		fmt.Println("Handling request...")
		fmt.Fprintf(w, "bar")
	})
	err = http.ListenAndServe(":10002", nil)
	if err != nil {
		glog.Fatal(err)
	}
}
```

### Serverside dialing of unreachable machine
Now that the pipeline is setup, we can dial that previously unreachable machine and get a 
tcp connection out of it:
```go
func main() {
	flag.Parse()

	dialer := operator.NewDialer(nil)
	dialer.OperatorResolver.SetOperator("unreachable1", "myserver1.example.com")
	tr := &http.Transport{DialContext: dialer.DialContext()}

    client := &http.Client{Transport: tr}
    // Note the format of the address here. The resolving is tweaked to work seemlessly.
    resp, err := client.Get("http://unreachable1.my-service/foo")
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
```
