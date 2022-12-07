package queue

import (
	"fmt"
	"net/http"
	"strings"
	//"encoding/json"
	"bytes"
	"crypto/tls"
	class "github.com/matehaxor03/holistic_db_client/class"
	json "github.com/matehaxor03/holistic_json/json"
	"io/ioutil"
	"sync"
	"time"
)

type QueueServer struct {
	Start func() []error
}

func NewQueueServer(port string, server_crt_path string, server_key_path string, processor_domain_name string, processor_port string) (*QueueServer, []error) {
	struct_type := "*queue.QueueServer"
	var errors []error
	wait_groups := make(map[string]*(sync.WaitGroup))
	result_groups := make(map[string](*json.Map))
	//var this_holisic_queue_server *HolisticQueueServer

	client_manager, client_manager_errors := class.NewClientManager()
	if client_manager_errors != nil {
		return nil, client_manager_errors
	}

	test_read_client, test_read_client_errors := client_manager.GetClient("holistic_db_config:127.0.0.1:3306:holistic:holistic_read")
	if test_read_client_errors != nil {
		return nil, test_read_client_errors
	}
	
	test_read_database, test_read_database_errors := test_read_client.GetDatabase()
	if test_read_database_errors != nil {
		return nil, test_read_database_errors
	}

	queues := make(map[string](*Queue))
	table_names, table_names_errors := test_read_database.GetTableNames()
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
	

	domain_name, domain_name_errors := class.NewDomainName(processor_domain_name)
	if domain_name_errors != nil {
		errors = append(errors, domain_name_errors...)
	}

	//todo: add filters to fields
	data := json.Map{
		"[fields]": json.Map{},
		"[schema]": json.Map{},
		"[system_fields]": json.Map{
			"[port]":&port,
			"[server_crt_path]":&server_crt_path,
			"[server_key_path]":&server_key_path,
		},
		"[system_schema]":json.Map{
			"[port]": json.Map{"type":"string"},
			"[server_crt_path]": json.Map{"type":"string"},
			"[server_key_path]": json.Map{"type":"string"},
		},
	}

	getData := func() *json.Map {
		return &data
	}

	
	getPort := func() (string, []error) {
		temp_value, temp_value_errors := class.GetField(struct_type, getData(), "[system_schema]", "[system_fields]", "[port]", "string")
		if temp_value_errors != nil {
			return "",temp_value_errors
		}
		return temp_value.(string), nil
	}

	getServerCrtPath := func() (string, []error) {
		temp_value, temp_value_errors := class.GetField(struct_type, getData(), "[system_schema]", "[system_fields]", "[server_crt_path]", "string")
		if temp_value_errors != nil {
			return "",temp_value_errors
		}
		return temp_value.(string), nil
	}

	getServerKeyPath := func() (string, []error) {
		temp_value, temp_value_errors := class.GetField(struct_type, getData(), "[system_schema]", "[system_fields]", "[server_key_path]", "string")
		if temp_value_errors != nil {
			return "",temp_value_errors
		}
		return temp_value.(string), nil
	}


	validate := func() []error {
		return class.ValidateData(getData(), "HolisticQueueServer")
	}

	domain_name_value, domain_name_value_errors := domain_name.GetDomainName()
	if domain_name_value_errors != nil {
		return nil, domain_name_value_errors
	}

	processor_url := fmt.Sprintf("https://%s:%s/", domain_name_value, processor_port)
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

	write_response := func(w http.ResponseWriter, result json.Map, write_response_errors []error) {
		if len(write_response_errors) > 0 {
			result.SetNil("data")
			result.SetErrors("[errors]", &write_response_errors)
		}

		var json_payload_builder strings.Builder
		result_as_string_errors := result.ToJSONString(&json_payload_builder)
		if result_as_string_errors != nil {
			write_response_errors = append(write_response_errors, result_as_string_errors...)
		}
		
		w.Header().Set("Content-Type", "application/json")
		if result_as_string_errors == nil {
			w.Write([]byte(json_payload_builder.String()))
		} else {
			w.Write([]byte(fmt.Sprintf("{\"[errors]\":\"%s\", \"data\":null}", strings.ReplaceAll(fmt.Sprintf("%s", result_as_string_errors), "\"", "\\\""))))
		}
	}

	wakeup_processor := func(queue_type *string) []error {
		var wakeup_processor_errors []error

		wakeup_payload := json.Map{}
		wakeup_payload.SetString("[queue]", queue_type)
		wakeup_queue_mode := "WakeUp"
		wakeup_payload.SetString("[queue_mode]", &wakeup_queue_mode)
		var json_payload_builder strings.Builder
		wakeup_payload_as_string_errors := wakeup_payload.ToJSONString(&json_payload_builder)

		if wakeup_payload_as_string_errors != nil {
			wakeup_processor_errors = append(wakeup_processor_errors, wakeup_payload_as_string_errors...)
		}

		if len(wakeup_processor_errors) > 0 {
			return wakeup_processor_errors
		}

		wakeup_request_json_bytes := []byte(json_payload_builder.String())
		wakeup_request_json_reader := bytes.NewReader(wakeup_request_json_bytes)
		wakeup_request, wakeup_request_error := http.NewRequest(http.MethodPost, processor_url, wakeup_request_json_reader)
		if wakeup_request_error != nil {
			wakeup_processor_errors = append(wakeup_processor_errors, wakeup_request_error)
		}

		wakeup_http_response, wakeup_http_response_error := http_client.Do(wakeup_request)
		if wakeup_http_response_error != nil {
			wakeup_processor_errors = append(wakeup_processor_errors, wakeup_http_response_error)
		}

		if len(wakeup_processor_errors) > 0 {
			return wakeup_processor_errors
		}

		wakeup_response_body_payload, wakeup_response_body_payload_error := ioutil.ReadAll(wakeup_http_response.Body)
		if wakeup_response_body_payload_error != nil {
			wakeup_processor_errors = append(wakeup_processor_errors, wakeup_response_body_payload_error)
		} else if wakeup_response_body_payload == nil {
			wakeup_processor_errors = append(wakeup_processor_errors, fmt.Errorf("response to wakeup processor is nil"))
		} 

		if len(wakeup_processor_errors) > 0 {
			return wakeup_processor_errors
		}

		return nil
	}

	processRequest := func(w http.ResponseWriter, req *http.Request) {
		var process_request_errors []error
		result := json.Map{}

		if !(req.Method == "POST" || req.Method == "PATCH" || req.Method == "PUT") {
			process_request_errors = append(process_request_errors, fmt.Errorf("request method not supported: " + req.Method))
		}

		if len(process_request_errors) > 0 {
			write_response(w, result, process_request_errors)
			return
		}

		body_payload, body_payload_error := ioutil.ReadAll(req.Body)
		if body_payload_error != nil {
			process_request_errors = append(process_request_errors, body_payload_error)
		}

		if len(process_request_errors) > 0 {
			write_response(w, result, process_request_errors)
			return
		}

		
		json_payload, json_payload_errors := json.ParseJSON(string(body_payload))
		if json_payload_errors != nil {
			process_request_errors = append(process_request_errors, json_payload_errors...)
		}

		if json_payload == nil {
			process_request_errors = append(process_request_errors, fmt.Errorf("json_payload is nil"))
		}

		if len(process_request_errors) > 0 {
			write_response(w, result, process_request_errors)
			return
		}

		queue_type, queue_type_errors := json_payload.GetString("[queue]")
		trace_id, trace_id_errors := json_payload.GetString("[trace_id]")

		if queue_type_errors != nil {
			process_request_errors = append(process_request_errors, queue_type_errors...)
		}

		if queue_type == nil {
			process_request_errors = append(process_request_errors, fmt.Errorf("[queue] has nil value"))
		}

		if trace_id_errors != nil {
			process_request_errors = append(process_request_errors, trace_id_errors...)
		}

		if trace_id == nil {
			process_request_errors = append(process_request_errors, fmt.Errorf("[trace_id] has nil value"))
		}
		

		if len(process_request_errors) > 0 {
			fmt.Println("error " + string(body_payload))
			write_response(w, result, process_request_errors)
			return
		} else {
			//fmt.Println("no error " + string(body_payload))
		}

		queue, queue_found := queues[*queue_type]
		if !queue_found {	
			process_request_errors = append(process_request_errors, fmt.Errorf("[queue] %s not found", *queue_type))
		}

		if queue == nil {
			process_request_errors = append(process_request_errors, fmt.Errorf("[queue] %s is nil", *queue_type))
		}

		queue_mode, queue_mode_errors := json_payload.GetString("[queue_mode]")
		if queue_mode_errors != nil {
			process_request_errors = append(process_request_errors, queue_mode_errors...)
		} 

		if len(process_request_errors) > 0 {
			write_response(w, result, process_request_errors)
			return
		}

		if *queue_mode == "PushBack" {
			var wg sync.WaitGroup
			wg.Add(1)
			wait_groups[*trace_id] = &wg
			queue.PushBack(json_payload)

			wakeup_processor_errors := wakeup_processor(queue_type)
			if wakeup_processor_errors != nil {
				process_request_errors = append(process_request_errors, wakeup_processor_errors...)
			}	
			
			if len(process_request_errors) > 0 {
				write_response(w, result, process_request_errors)
				return
			}

			result_ptr, found := result_groups[*trace_id]
			if !found {
				fmt.Println("waiting")
				wg.Wait()
				fmt.Println("waked_up")
				result_ptr = result_groups[*trace_id]
			} else {
				fmt.Println("result found before waiting")
			}

			result = *result_ptr

			//wg.Wait()
			//result = *(result_groups[*trace_id])
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
			process_request_errors = append(process_request_errors, fmt.Errorf("[queue_mode] not supported please implement: %s", *queue_mode))
		}
	
		write_response(w, result, process_request_errors)
	}

	x := QueueServer{
		Start: func() []error {
			var start_server_errors []error
			http.HandleFunc("/", processRequest)

			temp_port, temp_port_errors := getPort()
			if temp_port_errors != nil {
				return temp_port_errors
			}

			temp_server_crt_path, temp_server_crt_path_errors := getServerCrtPath()
			if temp_server_crt_path_errors != nil {
				return temp_server_crt_path_errors
			}

			temp_server_key_path, temp_server_key_path_errors := getServerKeyPath()
			if temp_server_key_path_errors != nil {
				return temp_server_key_path_errors
			}

			err := http.ListenAndServeTLS(":"+ temp_port, temp_server_crt_path, temp_server_key_path, nil)
			if err != nil {
				start_server_errors = append(start_server_errors, err)
			}

			if len(start_server_errors) > 0 {
				return start_server_errors
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
