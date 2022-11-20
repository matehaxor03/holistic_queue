package main

import (
	queue "github.com/matehaxor03/holistic_queue/queue"
	"os"
	"fmt"
)

func main() {
	var errors []error
	queue_server, queue_server_errors := queue.NewQueueServer("5000", "server.crt", "server.key", "127.0.0.1", "5002")
	if queue_server_errors != nil {
		errors = append(errors, queue_server_errors...)	
	} else {
		queue_server_start_errors := queue_server.Start()
		if queue_server_start_errors != nil {
			errors = append(errors, queue_server_start_errors...)
		}
	}

	if len(errors) > 0 {
		fmt.Println(errors)
		os.Exit(1)
	}
	
	os.Exit(0)
}


