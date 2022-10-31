package queue

import (
	"net/http"
	"fmt"
	"strings"
	"encoding/json"
	"io/ioutil"
	class "github.com/matehaxor03/holistic_db_client/class"
)

type HolisticQueueServer struct {
	Start      			func() ([]error)
}

func NewHolisticQueueServer(port string, server_crt_path string, server_key_path string) (*HolisticQueueServer, []error) {
	var errors []error
	//var this_holisic_queue_server *HolisticQueueServer
	
	db_hostname, db_port_number, db_name, read_db_username, _, migration_details_errors := class.GetCredentialDetails("holistic_read")
	if migration_details_errors != nil {
		errors = append(errors, migration_details_errors...)
	}

	host, host_errors := class.NewHost(&db_hostname, &db_port_number)
	client, client_errors := class.NewClient(host, &read_db_username, nil)

	if host_errors != nil {
		errors = append(errors, host_errors...)
	}

	if client_errors != nil {
		errors = append(errors, client_errors...)
	}

	if len(errors) > 0 {
		return nil, errors
	}

	_, use_database_errors := client.UseDatabaseByName(db_name)
	if use_database_errors != nil {
		return nil, use_database_errors
	}
	
	queues := make(map[string](*Queue))
	queues["CreateRepository"] = NewQueue()

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
				message_type, message_type_errors := json_payload.GetString("message_type")
				if message_type_errors != nil {
					w.Write([]byte("message_type does not exist error"))
				} else {
					queue, ok := queues[*message_type]
					if ok {
						queue.PushBack(&json_payload)
						w.Write([]byte("ok"))
					} else {
						fmt.Println(fmt.Sprintf("message type not supported please implement: %s", *message_type))
						w.Write([]byte(fmt.Sprintf("message type not supported please implement: %s", *message_type)))
					}
				}
			}
		} else {
			w.Write([]byte(formatRequest(req)))
		}
	}

	x := HolisticQueueServer{
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
