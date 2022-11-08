package queue

import (
	"net/http"
	"fmt"
	"strings"
	"encoding/json"
	"io/ioutil"
	"sync"
	class "github.com/matehaxor03/holistic_db_client/class"
)

type QueueServer struct {
	Start      			func() ([]error)
}

func NewQueueServer(port string, server_crt_path string, server_key_path string) (*QueueServer, []error) {
	var errors []error
	wait_groups := make(map[string]*(sync.WaitGroup))
	result_groups := make(map[string]class.Map)
	//var this_holisic_queue_server *HolisticQueueServer
	
	database, database_errors := class.GetDatabase("holistic_read")
	if database_errors != nil {
		errors = append(errors, database_errors...)
	}

	if len(errors) > 0 {
		return nil, errors
	}
	
	queues := make(map[string](*Queue))
	table_names, table_names_errors := database.GetTableNames()
	if table_names_errors != nil {
		return nil, table_names_errors
	}

	for _, table_name := range *table_names {
		queues["Create_" + table_name] = NewQueue()
		queues["Read_" + table_name] = NewQueue()
		queues["Update_" + table_name] = NewQueue()
		queues["Delete_" + table_name] = NewQueue()
	}

	queues["GetTableNames"] = NewQueue()

	//todo: add filters to fields
	data := class.Map{
		"[port]": class.Map{"value": class.CloneString(&port), "mandatory": true},
		"[server_crt_path]": class.Map{"value": class.CloneString(&server_crt_path), "mandatory": true},
		"[server_key_path]": class.Map{"value": class.CloneString(&server_key_path), "mandatory": true},
	}

	getPort := func() *string {
		port, _ := data.M("[port]").GetString("value")
		return class.CloneString(port)
	}

	getServerCrtPath := func() *string {
		crt, _ := data.M("[server_crt_path]").GetString("value")
		return class.CloneString(crt)
	}

	getServerKeyPath := func() *string {
		key, _ := data.M("[server_key_path]").GetString("value")
		return class.CloneString(key)
	}

	validate := func() []error {
		return class.ValidateData(data, "HolisticQueueServer")
	}

	/*
	setHolisticQueueServer := func(holisic_queue_server *HolisticQueueServer) {
		this_holisic_queue_server = holisic_queue_server
	}*/

	/*
	getHolisticQueueServer := func() *HolisticQueueServer {
		return this_holisic_queue_server
	}*/

	formatRequest := func(r *http.Request) string {
		var request []string
	
		url := fmt.Sprintf("%v %v %v", r.Method, r.URL, r.Proto)
		request = append(request, url)
		request = append(request, fmt.Sprintf("Host: %v", r.Host))
		for name, headers := range r.Header {
			name = strings.ToLower(name)
			for _, h := range headers {
				request = append(request, fmt.Sprintf("%v: %v", name, h))
			}
		}
	
		if r.Method == "POST" {
			r.ParseForm()
			request = append(request, "\n")
			request = append(request, r.Form.Encode())
		}
	
		return strings.Join(request, "\n")
	}

	processRequest := func(w http.ResponseWriter, req *http.Request) {
		if req.Method == "POST" || req.Method == "PATCH" || req.Method == "PUT" {
			json_payload := class.Map{}
			body_payload, body_payload_error := ioutil.ReadAll(req.Body);
			if body_payload_error != nil {
				w.Write([]byte(body_payload_error.Error()))
			} else {
				json.Unmarshal([]byte(body_payload), &json_payload)
				
				fmt.Println(json_payload.Keys())
				fmt.Println(string(body_payload))

				message_type, message_type_errors := json_payload.GetString("[queue]")
				trace_id, _ := json_payload.GetString("[trace_id]")

				if message_type_errors != nil {
					w.Write([]byte("[queue] does not exist error"))
				} else {
					queue, ok := queues[*message_type]
					if ok {
						queue_mode, queue_mode_errors := json_payload.GetString("[queue_mode]")
						if queue_mode_errors != nil {
							w.Write([]byte("[queue_mode] does not exist error"))
						} else {
							if *queue_mode == "PushBack" {
								var wg sync.WaitGroup
								wg.Add(1)
								wait_groups[*trace_id] = &wg
								queue.PushBack(&json_payload)
								wg.Wait()

								w.Write([]byte(result_groups[*trace_id].ToJSONString()))
								delete(result_groups, *trace_id)
							} else if *queue_mode == "GetAndRemoveFront" {
								front := queue.GetAndRemoveFront()
								if front == nil {
									w.Write([]byte("{}"))
								} else {
									w.Write([]byte(front.ToJSONString()))
								}
							} else if *queue_mode == "complete" {
								result_groups[*trace_id] = json_payload
								(wait_groups[*trace_id]).Done()
								delete(wait_groups, *trace_id)
							} else {
								fmt.Println(fmt.Sprintf("[queue_mode] not supported please implement: %s", *queue_mode))
								w.Write([]byte(fmt.Sprintf("[queue_mode] not supported please implement: %s", *queue_mode)))
							}
						}
					} else {
						fmt.Println(fmt.Sprintf("[queue] not supported please implement: %s", *message_type))
						w.Write([]byte(fmt.Sprintf("[queue] not supported please implement: %s", *message_type)))
					}
				}
			}
		} else {
			w.Write([]byte(formatRequest(req)))
		}
	}

	x := QueueServer{
		Start: func() []error {
			var errors []error
			http.HandleFunc("/", processRequest)

			err := http.ListenAndServeTLS(":" + *(getPort()), *(getServerCrtPath()), *(getServerKeyPath()), nil)
			if err != nil {
				errors = append(errors, err)
			}

			if len(errors) > 0 {
				return errors
			}

			return nil
		},
	}
	//setHolisticQueueServer(&x)

	validate_errors := validate()
	if validate_errors != nil {
		errors = append(errors, validate_errors...)
	}

	if len(errors) > 0 {
		return nil, errors
	}

	return &x, nil
}
