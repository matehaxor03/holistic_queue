package main

import (
	queue "github.com/matehaxor03/holistic_queue/queue"
	"os"
)

func main() {
	var errors []error
	holistic_queue_server, holistic_queue_server_errors := queue.NewHolisticQueueServer("5000", "server.crt", "server.key")
	if errors != nil {
		errors = append(errors, holistic_queue_server_errors...)	
	} else {
		holistic_queue_start_errors := holistic_queue_server.Start()
		if holistic_queue_start_errors != nil {
			errors = append(errors, holistic_queue_start_errors...)
		}
	}

	if len(errors) > 0 {
		os.Exit(1)
	}
	
	os.Exit(0)
}


