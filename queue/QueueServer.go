package queue

import (
	"fmt"
	"net/http"
	"strings"
	//"encoding/json"
	"bytes"
	"crypto/tls"
	class "github.com/matehaxor03/holistic_db_client/class"
	common "github.com/matehaxor03/holistic_common/common"
	json "github.com/matehaxor03/holistic_json/json"
	thread_safe "github.com/matehaxor03/holistic_thread_safe/thread_safe"
	http_extension "github.com/matehaxor03/holistic_http/http_extension"

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

	queues := make(map[string](*thread_safe.Queue))
	table_names, table_names_errors := test_read_database.GetTableNames()
	if table_names_errors != nil {
		return nil, table_names_errors
	}

	for _, table_name := range *table_names {
		queues["CreateRecords_"+table_name] = thread_safe.NewQueue()
		queues["CreateRecord_"+table_name] = thread_safe.NewQueue()
		queues["ReadRecords_"+table_name] = thread_safe.NewQueue()
		queues["UpdateRecords_"+table_name] = thread_safe.NewQueue()
		queues["UpdateRecord_"+table_name] = thread_safe.NewQueue()
		queues["CreateRecords_"+table_name] = thread_safe.NewQueue()
		queues["DeleteRecords_"+table_name] = thread_safe.NewQueue()
		queues["GetSchema_"+table_name] = thread_safe.NewQueue()
		queues["Run_BuildBranchInstance"] = thread_safe.NewQueue()
	}

	queues["GetTableNames"] = thread_safe.NewQueue()
	

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

	wakeup_processor := func(queue string, trace_id string) []error {
		var wakeup_processor_errors []error

		wakeup_payload := json.Map{queue: json.Map{"[queue_mode]":"WakeUp", "[trace_id]":trace_id}}
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
		} else {
			fmt.Println(string(wakeup_response_body_payload))
		}

		if len(wakeup_processor_errors) > 0 {
			return wakeup_processor_errors
		}


		// check body payload

		return nil
	}

	processRequest := func(w http.ResponseWriter, req *http.Request) {
		var process_request_errors []error
		result := json.Map{}

		if !(req.Method == "POST" || req.Method == "PATCH" || req.Method == "PUT") {
			process_request_errors = append(process_request_errors, fmt.Errorf("request method not supported: " + req.Method))
		}

		if len(process_request_errors) > 0 {
			http_extension.WriteResponse(w, result, process_request_errors)
			return
		}

		body_payload, body_payload_error := ioutil.ReadAll(req.Body)
		if body_payload_error != nil {
			process_request_errors = append(process_request_errors, body_payload_error)
		}

		if len(process_request_errors) > 0 {
			http_extension.WriteResponse(w, result, process_request_errors)
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
			http_extension.WriteResponse(w, result, process_request_errors)
			return
		}

		keys := json_payload.Keys()
		if len(keys) != 1 {
			process_request_errors = append(process_request_errors, fmt.Errorf("root level keys is more than 1"))
			http_extension.WriteResponse(w, result, process_request_errors)
			return
		}

		queue := keys[0]
		json_pay_load_params, json_pay_load_params_errors := json_payload.GetMap(queue)
		if json_pay_load_params_errors != nil {
			process_request_errors = append(process_request_errors, fmt.Errorf("did not find map for queue type %s", queue))
			http_extension.WriteResponse(w, result, process_request_errors)
			return
		} else if common.IsNil(json_pay_load_params) {
			process_request_errors = append(process_request_errors, fmt.Errorf("json payload is nill for queue type %s", queue))
			http_extension.WriteResponse(w, result, process_request_errors)
			return
		}

		trace_id, trace_id_errors := json_pay_load_params.GetString("[trace_id]")

		if queue == "" {
			process_request_errors = append(process_request_errors, fmt.Errorf("[queue] has empty value"))
		}

		if trace_id_errors != nil {
			process_request_errors = append(process_request_errors, trace_id_errors...)
		}

		if trace_id == nil {
			process_request_errors = append(process_request_errors, fmt.Errorf("[trace_id] has nil value"))
		}
		
		if len(process_request_errors) > 0 {
			http_extension.WriteResponse(w, result, process_request_errors)
			return
		} 

		queue_obj, queue_found := queues[queue]
		if !queue_found {	
			process_request_errors = append(process_request_errors, fmt.Errorf("[queue] %s not found", queue))
		} else if queue_obj == nil {
			process_request_errors = append(process_request_errors, fmt.Errorf("[queue] %s is nil", queue))
		}

		queue_mode, queue_mode_errors := json_pay_load_params.GetStringValue("[queue_mode]")
		if queue_mode_errors != nil {
			process_request_errors = append(process_request_errors, queue_mode_errors...)
		} else if queue_mode == "" {
			queue_mode = "PushBack"
			json_pay_load_params.SetStringValue("[queue_mode]", queue_mode)
		}

		async, async_errors := json_pay_load_params.GetBool("[async]")
		if async_errors != nil {
			process_request_errors = append(process_request_errors, async_errors...)
		} else if common.IsNil(async) {
			async_false := false
			async = &async_false
			json_pay_load_params.SetBool("[async]", &async_false)
		}

		if len(process_request_errors) > 0 {
			http_extension.WriteResponse(w, result, process_request_errors)
			return
		}

		result_inner := json.Map{"[trace_id]":*trace_id, "[queue_mode]":queue_mode, "[async]":*async}
		result = json.Map{queue: result_inner}
		
		if queue_mode == "PushBack" {
			var wg sync.WaitGroup
			if !json_pay_load_params.IsBoolTrue("[async]") {
				wg.Add(1)
				wait_groups[*trace_id] = &wg
			}
			println(string(body_payload))
			
			queue_obj.PushBack(json_payload)

			wakeup_processor_errors := wakeup_processor(queue, *trace_id)
			if wakeup_processor_errors != nil {
				process_request_errors = append(process_request_errors, wakeup_processor_errors...)
			}	
			
			if len(process_request_errors) > 0 {
				http_extension.WriteResponse(w, result, process_request_errors)
				return
			}

			if !json_pay_load_params.IsBoolTrue("[async]") {
				result_ptr, found := result_groups[*trace_id]
				if !found {
					wg.Wait()
					result_ptr = result_groups[*trace_id]
				}
				result = *result_ptr
				delete(result_groups, *trace_id)
			}
		} else if queue_mode == "GetAndRemoveFront" {
			front := queue_obj.GetAndRemoveFront()
			if front != nil {
				result = *front
			} else {
				empty_map := json.Map{}
				empty_map.SetMap("empty", &result_inner)
				result = empty_map
			}
		} else if queue_mode == "complete" {
			if !json_pay_load_params.IsBoolTrue("[async]") {
				json_pay_load_params.RemoveKey("[queue_mode]")
				json_pay_load_params.RemoveKey("[queue]")
				fmt.Println(string(body_payload))
				result_groups[*trace_id] = json_payload
				wait_group, wait_group_found := wait_groups[*trace_id]
				if wait_group_found {
					wait_group.Done()
					delete(wait_groups, *trace_id)
				}
			}
		} else {
			process_request_errors = append(process_request_errors, fmt.Errorf("[queue_mode] not supported please implement: %s", queue_mode))
		}

		http_extension.WriteResponse(w, result, process_request_errors)
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
