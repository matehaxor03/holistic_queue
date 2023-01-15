package queue

import (
	"fmt"
	"net/http"
	"strings"
	"bytes"
	"crypto/tls"
	validate "github.com/matehaxor03/holistic_db_client/validate"
	dao "github.com/matehaxor03/holistic_db_client/dao"
	common "github.com/matehaxor03/holistic_common/common"
	json "github.com/matehaxor03/holistic_json/json"
	thread_safe "github.com/matehaxor03/holistic_thread_safe/thread_safe"
	http_extension "github.com/matehaxor03/holistic_http/http_extension"
	helper "github.com/matehaxor03/holistic_db_client/helper"


	"io/ioutil"
	"sync"
	"time"
)

type QueueServer struct {
	Start func() []error
}

func NewQueueServer(port string, server_crt_path string, server_key_path string, processor_domain_name string, processor_port string) (*QueueServer, []error) {
	verfiy := validate.NewValidator()
	var errors []error
	lock_wait_group := &sync.Mutex{}
	wait_groups := make(map[string]*(sync.WaitGroup))
	lock_result_group := &sync.Mutex{}
	result_groups := make(map[string](*json.Map))

	client_manager, client_manager_errors := dao.NewClientManager()
	if client_manager_errors != nil {
		return nil, client_manager_errors
	}

	test_read_client, test_read_client_errors := client_manager.GetClient("127.0.0.1", "3306", "holistic", "holistic_r")
	if test_read_client_errors != nil {
		return nil, test_read_client_errors
	}
	
	test_read_database := test_read_client.GetDatabase()

	queues := make(map[string](*thread_safe.Queue))
	table_names, table_names_errors := test_read_database.GetTableNames()
	if table_names_errors != nil {
		return nil, table_names_errors
	}

	queues["Run_Sync"] = thread_safe.NewQueue()


	for _, table_name := range table_names {
		queues["CreateRecords_"+table_name] = thread_safe.NewQueue()
		queues["CreateRecord_"+table_name] = thread_safe.NewQueue()
		queues["ReadRecords_"+table_name] = thread_safe.NewQueue()
		queues["UpdateRecords_"+table_name] = thread_safe.NewQueue()
		queues["UpdateRecord_"+table_name] = thread_safe.NewQueue()
		queues["CreateRecords_"+table_name] = thread_safe.NewQueue()
		queues["DeleteRecords_"+table_name] = thread_safe.NewQueue()
		queues["GetSchema_"+table_name] = thread_safe.NewQueue()
	}

	queues["Run_StartBuildBranchInstance"] = thread_safe.NewQueue()
	queues["Run_NotStarted"] = thread_safe.NewQueue()
	queues["Run_Start"] = thread_safe.NewQueue()
	queues["Run_CreateSourceFolder"] = thread_safe.NewQueue()
	queues["Run_CreateDomainNameFolder"] = thread_safe.NewQueue()
	queues["Run_CreateRepositoryAccountFolder"] = thread_safe.NewQueue()
	queues["Run_CreateRepositoryFolder"] = thread_safe.NewQueue()
	queues["Run_CreateBranchesFolder"] = thread_safe.NewQueue()
	queues["Run_CreateTagsFolder"] = thread_safe.NewQueue()
	queues["Run_CreateBranchInstancesFolder"] = thread_safe.NewQueue()
	queues["Run_CreateTagInstancesFolder"] = thread_safe.NewQueue()
	queues["Run_CreateBranchOrTagFolder"] = thread_safe.NewQueue()
	queues["Run_CloneBranchOrTagFolder"] = thread_safe.NewQueue()
	queues["Run_PullLatestBranchOrTagFolder"] = thread_safe.NewQueue()
	queues["Run_CreateInstanceFolder"] = thread_safe.NewQueue()
	queues["Run_CopyToInstanceFolder"] = thread_safe.NewQueue()
	queues["Run_CreateGroup"] = thread_safe.NewQueue()
	queues["Run_CreateUser"] = thread_safe.NewQueue()
	queues["Run_AssignGroupToUser"] = thread_safe.NewQueue()
	queues["Run_AssignGroupToInstanceFolder"] = thread_safe.NewQueue()
	queues["Run_Clean"] = thread_safe.NewQueue()
	queues["Run_Lint"] = thread_safe.NewQueue()
	queues["Run_Build"] = thread_safe.NewQueue()
	queues["Run_UnitTests"] = thread_safe.NewQueue()
	queues["Run_IntegrationTests"] = thread_safe.NewQueue()
	queues["Run_IntegrationTestSuite"] = thread_safe.NewQueue()
	queues["Run_RemoveGroupFromInstanceFolder"] = thread_safe.NewQueue()
	queues["Run_RemoveGroupFromUser"] = thread_safe.NewQueue()
	queues["Run_DeleteGroup"] = thread_safe.NewQueue()
	queues["Run_DeleteUser"] = thread_safe.NewQueue()
	queues["Run_DeleteInstanceFolder"] = thread_safe.NewQueue()
	queues["Run_End"] = thread_safe.NewQueue()

	queues["GetTableNames"] = thread_safe.NewQueue()
	

	domain_name, domain_name_errors := dao.NewDomainName(verfiy, processor_domain_name)
	if domain_name_errors != nil {
		errors = append(errors, domain_name_errors...)
	}

	//todo: add filters to fields
	data := json.NewMapValue()
	data.SetMapValue("[fields]", json.NewMapValue())
	data.SetMapValue("[schema]", json.NewMapValue())

	map_system_fields := json.NewMapValue()
	map_system_fields.SetObjectForMap("[port]", port)
	map_system_fields.SetObjectForMap("[server_crt_path]", server_crt_path)
	map_system_fields.SetObjectForMap("[server_key_path]", server_key_path)
	data.SetMapValue("[system_fields]", map_system_fields)

	///

	//todo: add filters to fields

	map_system_schema := json.NewMapValue()
	
	map_port := json.NewMapValue()
	map_port.SetStringValue("type", "string")
	map_system_schema.SetMapValue("[port]", map_port)

	map_server_crt_path := json.NewMapValue()
	map_server_crt_path.SetStringValue("type", "string")
	map_system_schema.SetMapValue("[server_crt_path]", map_server_crt_path)

	map_server_key_path := json.NewMapValue()
	map_server_key_path.SetStringValue("type", "string")
	map_system_schema.SetMapValue("[server_key_path]", map_server_key_path)

	map_queue_port := json.NewMapValue()
	map_queue_port.SetStringValue("type", "string")
	map_system_schema.SetMapValue("[server_key_path]", map_queue_port)

	data.SetMapValue("[system_schema]", map_system_schema)

	getData := func() *json.Map {
		return &data
	}

	
	getPort := func() (string, []error) {
		temp_value, temp_value_errors := helper.GetField(*getData(), "[system_schema]", "[system_fields]", "[port]", "string")
		if temp_value_errors != nil {
			return "",temp_value_errors
		}
		return temp_value.(string), nil
	}

	getServerCrtPath := func() (string, []error) {
		temp_value, temp_value_errors := helper.GetField(*getData(), "[system_schema]", "[system_fields]", "[server_crt_path]", "string")
		if temp_value_errors != nil {
			return "",temp_value_errors
		}
		return temp_value.(string), nil
	}

	getServerKeyPath := func() (string, []error) {
		temp_value, temp_value_errors := helper.GetField(*getData(), "[system_schema]", "[system_fields]", "[server_key_path]", "string")
		if temp_value_errors != nil {
			return "",temp_value_errors
		}
		return temp_value.(string), nil
	}

	validate := func() []error {
		return dao.ValidateData(getData(), "HolisticQueueServer")
	}

	domain_name_value := domain_name.GetDomainName()

	processor_url := fmt.Sprintf("https://%s:%s/", domain_name_value, processor_port)
	transport_config := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	http_client := http.Client{
		Timeout:   120 * time.Second,
		Transport: transport_config,
	}

	crud_wait_group := func(trace_id string, wait_group *(sync.WaitGroup), mode string)  (*(sync.WaitGroup), []error) {
		lock_wait_group.Lock()
		defer lock_wait_group.Unlock()
		var errors []error
		if mode == "create" {
			if common.IsNil(wait_group) {
				errors = append(errors, fmt.Errorf("cannot add nil wait_group"))
				return nil, errors
			}

			_, get_wait_group_found := wait_groups[trace_id]
			if get_wait_group_found {
				errors = append(errors, fmt.Errorf("trace_id already exists"))
				return nil, errors
			}
			wait_groups[trace_id] = wait_group
			return nil, nil
		} else if mode == "read" {
			get_wait_group, get_wait_group_found := wait_groups[trace_id]
			if get_wait_group_found {
				return get_wait_group, nil
			}
			return nil, nil
		} else if mode == "done-delete" {
			get_wait_group, get_wait_group_found := wait_groups[trace_id]
			if get_wait_group_found {
				get_wait_group.Done()
				delete(wait_groups, trace_id)
			}
			return nil, nil
		} else if mode == "delete" {
			_, get_wait_group_found := wait_groups[trace_id]
			if get_wait_group_found {
				delete(wait_groups, trace_id)
			}
			return nil, nil
		} else {
			errors = append(errors, fmt.Errorf("mode %s is not supported", mode))
			return nil, errors
		}
	}

	crud_result_group := func(trace_id string, result_group *json.Map, mode string)  (*json.Map, []error) {
		lock_result_group.Lock()
		defer lock_result_group.Unlock()
		var errors []error
		if mode == "create" {
			if common.IsNil(result_group) {
				errors = append(errors, fmt.Errorf("cannot add nil result group"))
				return nil, errors
			}

			_, get_result_group_found := result_groups[trace_id]
			if get_result_group_found {
				errors = append(errors, fmt.Errorf("trace_id already exists"))
				return nil, errors
			}

			result_groups[trace_id] = result_group
			return nil, nil
		} else if mode == "read" {
			get_result_group, get_result_group_found := result_groups[trace_id]
			if get_result_group_found {
				return get_result_group, nil
			}
			return nil, nil
		} else if mode == "delete" {
			_, get_result_group_found := result_groups[trace_id]
			if get_result_group_found {
				delete(result_groups, trace_id)
			}
			return nil, nil
		} else {
			errors = append(errors, fmt.Errorf("mode %s is not supported", mode))
			return nil, errors
		}
	}

	wakeup_processor := func(queue string, trace_id string) []error {
		var wakeup_processor_errors []error

		wakeup_payload_map := map[string]interface{}{"[queue]":queue, "[queue_mode]":"WakeUp", "[trace_id]":trace_id}
		wakeup_payload := json.NewMapOfValues(&wakeup_payload_map)
		var json_payload_builder strings.Builder
		wakeup_payload_as_string_errors := wakeup_payload.ToJSONString(&json_payload_builder)

		if wakeup_payload_as_string_errors != nil {
			wakeup_processor_errors = append(wakeup_processor_errors, wakeup_payload_as_string_errors...)
		}

		if len(wakeup_processor_errors) > 0 {
			fmt.Println(wakeup_processor_errors)
			return wakeup_processor_errors
		}

		wakeup_request_json_bytes := []byte(json_payload_builder.String())
		wakeup_request_json_reader := bytes.NewReader(wakeup_request_json_bytes)
		wakeup_request, wakeup_request_error := http.NewRequest(http.MethodPost, processor_url, wakeup_request_json_reader)
		if wakeup_request_error != nil {
			wakeup_processor_errors = append(wakeup_processor_errors, wakeup_request_error)
		}
		
		if len(wakeup_processor_errors) > 0 {
			fmt.Println(wakeup_processor_errors)
			return wakeup_processor_errors
		}

		http_client.Do(wakeup_request)
		return nil
		/*wakeup_http_response, wakeup_http_response_error := http_client.Do(wakeup_request)
		if wakeup_http_response_error != nil {
			wakeup_processor_errors = append(wakeup_processor_errors, wakeup_http_response_error)
		}

		if len(wakeup_processor_errors) > 0 {
			fmt.Println(wakeup_processor_errors)
			return wakeup_processor_errors
		}

		wakeup_response_body_payload, wakeup_response_body_payload_error := ioutil.ReadAll(wakeup_http_response.Body)
		if wakeup_response_body_payload_error != nil {
			wakeup_processor_errors = append(wakeup_processor_errors, wakeup_response_body_payload_error)
		} else if wakeup_response_body_payload == nil {
			wakeup_processor_errors = append(wakeup_processor_errors, fmt.Errorf("response to wakeup processor is nil"))
		} 

		if len(wakeup_processor_errors) > 0 {
			fmt.Println(wakeup_processor_errors)
			return wakeup_processor_errors
		}

		parse_waleup_response_json, parse_waleup_response_json_errors := json.Parse(string(wakeup_response_body_payload))
		if parse_waleup_response_json_errors != nil {
			wakeup_processor_errors = append(wakeup_processor_errors, parse_waleup_response_json_errors...)
		} else if common.IsNil(parse_waleup_response_json) {
			wakeup_processor_errors = append(wakeup_processor_errors, fmt.Errorf("parse_waleup_response_json is nil")) 
		}

		if len(wakeup_processor_errors) > 0 {
			fmt.Println(wakeup_processor_errors)
			return wakeup_processor_errors
		}

		json_wakeup_errors, json_wakeup_errors_errors := parse_waleup_response_json.GetErrors("[errors]")
		if json_wakeup_errors_errors != nil {
			wakeup_processor_errors = append(wakeup_processor_errors, json_wakeup_errors_errors...)
		}
		
		if !common.IsNil(json_wakeup_errors) {
			wakeup_processor_errors = append(wakeup_processor_errors, json_wakeup_errors...) 
		}

		if len(wakeup_processor_errors) > 0 {
			fmt.Println(wakeup_processor_errors)
			return wakeup_processor_errors
		}

		return nil*/
	}

	complete_request := func(request *json.Map) []error {
		var errors []error
		if common.IsNil(request) {
			errors = append(errors, fmt.Errorf("request is nil"))
			return errors
		}


		trace_id, trace_id_errors := request.GetString("[trace_id]")
		if trace_id_errors != nil {
			errors = append(errors, trace_id_errors...)
		} else if common.IsNil(trace_id) {
			errors = append(errors, fmt.Errorf("completed request [trace_id] is nil"))
		}

		if len(errors) > 0 {
			return errors
		}

		if !request.IsBoolTrue("[async]") {
			crud_result_group(*trace_id, request, "create")
			crud_wait_group(*trace_id, nil, "done-delete")
		}

		return nil
	}

	processRequest := func(w http.ResponseWriter, req *http.Request) {
		var process_request_errors []error
		
		if !(req.Method == "POST" || req.Method == "PATCH" || req.Method == "PUT") {
			process_request_errors = append(process_request_errors, fmt.Errorf("request method not supported: " + req.Method))
		}

		if len(process_request_errors) > 0 {
			http_extension.WriteResponse(w, json.Map{}, process_request_errors)
			return
		}

		body_payload, body_payload_error := ioutil.ReadAll(req.Body)
		if body_payload_error != nil {
			process_request_errors = append(process_request_errors, body_payload_error)
		}

		if len(process_request_errors) > 0 {
			http_extension.WriteResponse(w, json.Map{}, process_request_errors)
			return
		}
		
		request, request_errors := json.Parse(string(body_payload))
		if request_errors != nil {
			process_request_errors = append(process_request_errors, request_errors...)
		}

		if request == nil {
			process_request_errors = append(process_request_errors, fmt.Errorf("request is nil"))
		}

		if len(process_request_errors) > 0 {
			http_extension.WriteResponse(w, json.Map{}, process_request_errors)
			return
		}

		queue, queue_errors := request.GetString("[queue]")
		if queue_errors != nil {
			process_request_errors = append(process_request_errors, queue_errors...)
		} else if common.IsNil(queue) {
			process_request_errors = append(process_request_errors, fmt.Errorf("[queue] is nil"))
		}

		if len(process_request_errors) > 0 {
			http_extension.WriteResponse(w, json.Map{}, process_request_errors)
			return
		}

		trace_id, trace_id_errors := request.GetString("[trace_id]")

		if *queue == "" {
			process_request_errors = append(process_request_errors, fmt.Errorf("[queue] has empty value"))
		}

		if trace_id_errors != nil {
			process_request_errors = append(process_request_errors, trace_id_errors...)
		} else if common.IsNil(trace_id) {
			process_request_errors = append(process_request_errors, fmt.Errorf("[trace_id] is nil"))
		}

		if len(process_request_errors) > 0 {
			http_extension.WriteResponse(w, json.Map{}, process_request_errors)
			return
		} 

		queue_obj, queue_found := queues[*queue]
		if !queue_found {	
			process_request_errors = append(process_request_errors, fmt.Errorf("[queue] %s not found", *queue))
		} else if queue_obj == nil {
			process_request_errors = append(process_request_errors, fmt.Errorf("[queue] %s is nil", *queue))
		}

		queue_mode, queue_mode_errors := request.GetString("[queue_mode]")
		if queue_mode_errors != nil {
			process_request_errors = append(process_request_errors, queue_mode_errors...)
		} else if common.IsNil(queue_mode) {
			temp_queue_mode := "PushBack"
			request.SetString("[queue_mode]", &temp_queue_mode)
			queue_mode = &temp_queue_mode
		} 

		async, async_errors := request.GetBool("[async]")
		if async_errors != nil {
			process_request_errors = append(process_request_errors, async_errors...)
		} else if common.IsNil(async) {
			async_false := false
			async = &async_false
			request.SetBool("[async]", &async_false)
		}

		if len(process_request_errors) > 0 {
			http_extension.WriteResponse(w, *request, process_request_errors)
			return
		}
		
		if *queue_mode == "PushBack" {
			
			if !request.IsBoolTrue("[async]") {
				var wg sync.WaitGroup
				wg.Add(1)
				crud_wait_group(*trace_id, &wg, "create")
			}			
			
			queue_obj.PushBack(request)

			go wakeup_processor(*queue, *trace_id)

			if !request.IsBoolTrue("[async]") {
				//result_ptr, found := result_groups[*trace_id]
				get_wait_group, get_wait_group_errors := crud_wait_group(*trace_id, nil, "read")
				if get_wait_group_errors != nil {
					process_request_errors = append(process_request_errors, get_wait_group_errors...)
				}

				get_result_group, get_result_group_errors := crud_result_group(*trace_id, nil, "read")
				if get_result_group_errors != nil {
					process_request_errors = append(process_request_errors, get_result_group_errors...)
				}

				if len(process_request_errors) > 0 {
					http_extension.WriteResponse(w, *request, process_request_errors)
					return
				}

				if common.IsNil(get_result_group) && !common.IsNil(get_wait_group) {
					get_wait_group.Wait()
					get_result_group, get_result_group_errors = crud_result_group(*trace_id, nil, "read")
					if get_result_group_errors != nil {
						process_request_errors = append(process_request_errors, get_result_group_errors...)
					} else if common.IsNil(get_result_group) {
						process_request_errors = append(process_request_errors, fmt.Errorf("result group is nil"))
					}
				}

				if len(process_request_errors) > 0 {
					crud_result_group(*trace_id, nil, "delete")
					crud_wait_group(*trace_id, nil, "delete")
					http_extension.WriteResponse(w, *request, process_request_errors)
					return
				}
				
				request = get_result_group
				crud_result_group(*trace_id, nil, "delete")
				crud_wait_group(*trace_id, nil, "delete")
			}
		} else if *queue_mode == "GetAndRemoveFront" {
			front := queue_obj.GetAndRemoveFront()
			if front != nil {
				request = front
			} else {
				empty_map := map[string]interface{}{"[queue]":"empty", "[trace_id]":*trace_id, "[queue_mode]":queue_mode, "[async]":*async}
				empty_payload := json.NewMapOfValues(&empty_map)
				request = empty_payload
			}
		} else if *queue_mode == "complete" {
			complete_request(request)
			/*if !request.IsBoolTrue("[async]") {
				crud_result_group(*trace_id, request, "create")
				crud_wait_group(*trace_id, nil, "done-delete")
			}*/
		} else {
			process_request_errors = append(process_request_errors, fmt.Errorf("[queue_mode] not supported please implement: %s", *queue_mode))
		}

		
		cloned_request, cloned_request_errors := request.Clone()
		if cloned_request_errors != nil {
			process_request_errors = append(process_request_errors, cloned_request_errors...)
		} else if common.IsNil(cloned_request) {
			process_request_errors = append(process_request_errors, fmt.Errorf("cloned request is nil"))
		}

		http_extension.WriteResponse(w, *cloned_request, process_request_errors)
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
