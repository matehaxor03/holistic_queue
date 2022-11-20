package queue

import (
	"net/http"
	"fmt"
	"strings"
	//"encoding/json"
	"crypto/tls"
	"bytes"
	"time"
	"io/ioutil"
	"sync"
	class "github.com/matehaxor03/holistic_db_client/class"
)

type QueueServer struct {
	Start      			func() ([]error)
}

func NewQueueServer(port string, server_crt_path string, server_key_path string, processor_domain_name string, processor_port string) (*QueueServer, []error) {
	var errors []error
	wait_groups := make(map[string]*(sync.WaitGroup))
	result_groups := make(map[string](*class.Map))
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


	domain_name, domain_name_errors := class.NewDomainName(&processor_domain_name)
	if domain_name_errors != nil {
		errors = append(errors, domain_name_errors...)
	}


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

	processor_url := fmt.Sprintf("https://%s:%s/", *(domain_name.GetDomainName()), processor_port)
	transport_config := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	http_client := http.Client{
		Timeout: 120 * time.Second,
		Transport: transport_config,
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
		var errors []error
		if req.Method == "POST" || req.Method == "PATCH" || req.Method == "PUT" {
			body_payload, body_payload_error := ioutil.ReadAll(req.Body);
			if body_payload_error != nil {
				w.Write([]byte(body_payload_error.Error()))
			} else {
				json_payload, json_payload_errors := class.ParseJSON(string(body_payload))
				if json_payload_errors != nil {
					w.Write([]byte(fmt.Sprintf("%s", json_payload_errors)))
				} else {
					fmt.Println(json_payload.Keys())
					fmt.Println(string(body_payload))

					message_type, message_type_errors := json_payload.GetString("[queue]")
					trace_id, _ := json_payload.GetString("[trace_id]")

					if message_type_errors != nil {
						w.Write([]byte("[queue] does not exist error"))
					} else {
						fmt.Println(*message_type)
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
									queue.PushBack(json_payload)


									// wakeup the processor
									wakeup_payload := class.Map{}
									wakeup_payload.SetString("[queue]", message_type)
									wakeup_queue_mode := "WakeUp"
									wakeup_payload.SetString("[queue_mode]", &wakeup_queue_mode)
									wakeup_payload_as_string, wakeup_payload_as_string_errors := wakeup_payload.ToJSONString()

									if wakeup_payload_as_string_errors != nil {
										errors = append(errors, wakeup_payload_as_string_errors...)
									}

									wakeup_request_json_bytes := []byte(*wakeup_payload_as_string)
									wakeup_request_json_reader := bytes.NewReader(wakeup_request_json_bytes)
									wakeup_request, wakeup_request_error := http.NewRequest(http.MethodPost, processor_url, wakeup_request_json_reader)
									if wakeup_request_error != nil {
										errors = append(errors, wakeup_request_error)
									}

									wakeup_http_response, wakeup_http_response_error := http_client.Do(wakeup_request)
									if wakeup_http_response_error != nil {
										errors = append(errors, wakeup_http_response_error)
									} 

									wakeup_response_body_payload, wakeup_response_body_payload_error := ioutil.ReadAll(wakeup_http_response.Body)
									if wakeup_response_body_payload_error != nil {
										errors = append(errors, wakeup_response_body_payload_error)
									} else {
										fmt.Println(wakeup_response_body_payload)
									}

									if len(errors) > 0 {
										w.Write([]byte(fmt.Sprintf("%s", errors)))
										return
									}

									wg.Wait()

									result_as_string, result_as_string_errors := result_groups[*trace_id].ToJSONString()
									if result_as_string_errors != nil {
										w.Write([]byte(result_as_string_errors[0].Error()))
									} else {
										w.Write([]byte(*result_as_string))
									}
									delete(result_groups, *trace_id)
								} else if *queue_mode == "GetAndRemoveFront" {
									front := queue.GetAndRemoveFront()
									if front == nil {
										w.Write([]byte("{}"))
									} else {
										front_as_string, front_as_string_errors := front.ToJSONString()
										if front_as_string_errors != nil {
											w.Write([]byte(front_as_string_errors[0].Error()))
										} else {
											w.Write([]byte(*front_as_string))
										}
									}
								} else if *queue_mode == "complete" {
									json_payload.RemoveKey("[queue_mode]")
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
				//json.Unmarshal([]byte(body_payload), &json_payload)
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
