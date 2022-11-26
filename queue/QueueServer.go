package queue

import (
	"fmt"
	"net/http"
	"strings"
	//"encoding/json"
	"bytes"
	"crypto/tls"
	class "github.com/matehaxor03/holistic_db_client/class"
	"io/ioutil"
	"sync"
	"time"
)

type QueueServer struct {
	Start func() []error
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
		queues["Create_"+table_name] = NewQueue()
		queues["Read_"+table_name] = NewQueue()
		queues["Update_"+table_name] = NewQueue()
		queues["Delete_"+table_name] = NewQueue()
		queues["GetSchema_"+table_name] = NewQueue()
	}

	queues["GetTableNames"] = NewQueue()
	

	domain_name, domain_name_errors := class.NewDomainName(&processor_domain_name)
	if domain_name_errors != nil {
		errors = append(errors, domain_name_errors...)
	}

	//todo: add filters to fields
	data := class.Map{
		"[port]":            class.Map{"value": class.CloneString(&port), "mandatory": true},
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
		Timeout:   120 * time.Second,
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

		/*
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
	}*/

	write_response := func(w http.ResponseWriter, result class.Map, errors []error) {
		if len(errors) > 0 {
			result.SetNil("data")
			result.SetErrors("[errors]", &errors)
		}

		result_as_string, result_as_string_errors := result.ToJSONString()
		if result_as_string_errors != nil {
			errors = append(errors, result_as_string_errors...)
		}
		
		if result_as_string_errors == nil {
			w.Write([]byte(*result_as_string))
		} else {
			w.Write([]byte(fmt.Sprintf("{\"[errors]\":\"%s\", \"data\":null}", strings.ReplaceAll(fmt.Sprintf("%s", result_as_string_errors), "\"", "\\\""))))
		}
	}

	wakeup_processor := func(queue_type *string) []error {
		var errors []error

		wakeup_payload := class.Map{}
		wakeup_payload.SetString("[queue]", queue_type)
		wakeup_queue_mode := "WakeUp"
		wakeup_payload.SetString("[queue_mode]", &wakeup_queue_mode)
		wakeup_payload_as_string, wakeup_payload_as_string_errors := wakeup_payload.ToJSONString()

		if wakeup_payload_as_string_errors != nil {
			errors = append(errors, wakeup_payload_as_string_errors...)
		}

		if len(errors) > 0 {
			return errors
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

		if len(errors) > 0 {
			return errors
		}

		wakeup_response_body_payload, wakeup_response_body_payload_error := ioutil.ReadAll(wakeup_http_response.Body)
		if wakeup_response_body_payload_error != nil {
			errors = append(errors, wakeup_response_body_payload_error)
		} else if wakeup_response_body_payload == nil {
			errors = append(errors, fmt.Errorf("response to wakeup processor is nil"))
		} else {
			fmt.Println(wakeup_response_body_payload)
		}

		if len(errors) > 0 {
			return errors
		}

		return nil
	}

	processRequest := func(w http.ResponseWriter, req *http.Request) {
		var errors []error
		result := class.Map{}

		if !(req.Method == "POST" || req.Method == "PATCH" || req.Method == "PUT") {
			errors = append(errors, fmt.Errorf("request method not supported: " + req.Method))
		}

		if len(errors) > 0 {
			write_response(w, result, errors)
			return
		}

		body_payload, body_payload_error := ioutil.ReadAll(req.Body)
		if body_payload_error != nil {
			errors = append(errors, body_payload_error)
		}

		if len(errors) > 0 {
			write_response(w, result, errors)
			return
		}

		
		json_payload, json_payload_errors := class.ParseJSON(string(body_payload))
		if json_payload_errors != nil {
			errors = append(errors, json_payload_errors...)
		}

		if json_payload == nil {
			errors = append(errors, fmt.Errorf("json_payload is nil"))
		}

		if len(errors) > 0 {
			write_response(w, result, errors)
			return
		}

		queue_type, queue_type_errors := json_payload.GetString("[queue]")
		trace_id, trace_id_errors := json_payload.GetString("[trace_id]")

		if queue_type_errors != nil {
			errors = append(errors, queue_type_errors...)
		}

		if queue_type == nil {
			errors = append(errors, fmt.Errorf("[queue] has nil value"))
		}

		if trace_id_errors != nil {
			errors = append(errors, trace_id_errors...)
		}

		if trace_id == nil {
			errors = append(errors, fmt.Errorf("[trace_id] has nil value"))
		}
		

		if len(errors) > 0 {
			write_response(w, result, errors)
			return
		}

		fmt.Println(json_payload.Keys())
		fmt.Println(string(body_payload))

		queue, queue_found := queues[*queue_type]
		if !queue_found {	
			errors = append(errors, fmt.Errorf("[queue] %s not found", *queue_type))
		}

		if queue == nil {
			errors = append(errors, fmt.Errorf("[queue] %s is nil", *queue_type))
		}

		queue_mode, queue_mode_errors := json_payload.GetString("[queue_mode]")
		if queue_mode_errors != nil {
			errors = append(errors, queue_mode_errors...)
		} 

		if len(errors) > 0 {
			write_response(w, result, errors)
			return
		}

		if *queue_mode == "PushBack" {
			var wg sync.WaitGroup
			wg.Add(1)
			wait_groups[*trace_id] = &wg
			queue.PushBack(json_payload)

			wakeup_processor_errors := wakeup_processor(queue_type)
			if wakeup_processor_errors != nil {

			}			

			wg.Wait()
			result = *(result_groups[*trace_id])
			delete(result_groups, *trace_id)
		} else if *queue_mode == "GetAndRemoveFront" {
			front := queue.GetAndRemoveFront()
			if front != nil {
				result = *front
			} 
		} else if *queue_mode == "complete" {
			json_payload.RemoveKey("[queue_mode]")
			json_payload.RemoveKey("[queue]")
			result_groups[*trace_id] = json_payload
			(wait_groups[*trace_id]).Done()
			delete(wait_groups, *trace_id)
		} else {
			errors = append(errors, fmt.Errorf("[queue_mode] not supported please implement: %s", *queue_mode))
		}
	
		write_response(w, result, errors)
	}

	x := QueueServer{
		Start: func() []error {
			var errors []error
			http.HandleFunc("/", processRequest)

			err := http.ListenAndServeTLS(":"+*(getPort()), *(getServerCrtPath()), *(getServerKeyPath()), nil)
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
