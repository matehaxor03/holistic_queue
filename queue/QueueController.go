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


	"io/ioutil"
	"sync"
	"time"
)

type QueueController struct {
	GetCompleteFunction func() (*func(json.Map) []error) 
	GetNextMessageFunction func() (*func(string) (json.Map, []error))
	GetPushBackFunction func() (*func(json.Map) (*json.Map, []error))
	SetWakeupProcessorManagerFunction func(*func())
	GetProcessRequestFunction func() *func(w http.ResponseWriter, req *http.Request)
}

func NewQueueController(queue_name string, processor_domain_name string, processor_port string) (*QueueController, []error) {
	verfiy := validate.NewValidator()
	//var this_queue_controller *QueueController
	var errors []error
	lock_wait_group := &sync.Mutex{}
	wait_groups := make(map[string]*(sync.WaitGroup))
	lock_result_group := &sync.Mutex{}
	result_groups := make(map[string](*json.Map))
	get_next_message_lock := &sync.RWMutex{}
	complete_message_lock := &sync.RWMutex{}
	var processor_callback_function *func()
	var messageCountLock sync.Mutex
	var messageCount uint64

	queue_obj := thread_safe.NewQueue()

	domain_name, domain_name_errors := dao.NewDomainName(verfiy, processor_domain_name)
	if domain_name_errors != nil {
		errors = append(errors, domain_name_errors...)
	}

	domain_name_value := domain_name.GetDomainName()

	processor_url := fmt.Sprintf("https://%s:%s/processor_api/" + queue_name, domain_name_value, processor_port)
	transport_config := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	http_client := http.Client{
		Timeout:   120 * time.Second,
		Transport: transport_config,
	}

	incrementMessageCount := func() uint64 {
		messageCountLock.Lock()
		defer messageCountLock.Unlock()
		messageCount++
		return messageCount
	}

	/*
	set_queue_controller := func(queue_controller *QueueController) {
		this_queue_controller = queue_controller
	}*/

	/*get_queue_controller := func() *QueueController {
		return this_queue_controller
	}*/

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

	wakeup_processor := func() []error {
		wakeup_payload_map := map[string]interface{}{"[queue_mode]":"WakeUp"}
		wakeup_payload := json.NewMapOfValues(&wakeup_payload_map)
		
		if processor_callback_function != nil {
			(*processor_callback_function)()
			return nil
		}

		var wakeup_processor_errors []error

		
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

		wakeup_http_response, wakeup_http_response_error := http_client.Do(wakeup_request)
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

		return nil
	}

	get_next_message_from_queue := func(traceid string) (json.Map, []error) {
		get_next_message_lock.Lock()
		defer get_next_message_lock.Unlock()
		var errors []error
		var result json.Map
		
		if len(errors) > 0 {
			return json.NewMapValue(), errors
		}
		
		front := queue_obj.GetAndRemoveFront()
		if front != nil {
			result = *front
		} else {
			empty_map := map[string]interface{}{"[queue]":"empty", "[trace_id]":traceid, "[queue_mode]":"GetAndRemoveFront", "[async]":true}
			empty_payload := json.NewMapOfValues(&empty_map)
			result = *empty_payload
		}

		return result, nil
	}

	complete_request := func(request json.Map) []error {
		complete_message_lock.Lock()
		defer complete_message_lock.Unlock()
		var errors []error
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
			crud_result_group(*trace_id, &request, "create")
			crud_wait_group(*trace_id, nil, "done-delete")
		}

		return nil
	}

	push_back_process_request := func(request json.Map) (*json.Map, []error) {
		var errors []error

		trace_id, trace_id_errors := request.GetString("[trace_id]")
		if trace_id_errors != nil {
			errors = append(errors, trace_id_errors...)
		} else if common.IsNil(trace_id) {
			errors = append(errors, fmt.Errorf("completed request [trace_id] is nil"))
		}

		if len(errors) > 0 {
			return nil, errors
		}
		
		if !request.IsBoolTrue("[async]") {
			var wg sync.WaitGroup
			wg.Add(1)
			crud_wait_group(*trace_id, &wg, "create")
		}			
		
		queue_obj.PushBack(&request)

		go wakeup_processor()

		if !request.IsBoolTrue("[async]") {
			get_wait_group, get_wait_group_errors := crud_wait_group(*trace_id, nil, "read")
			if get_wait_group_errors != nil {
				errors = append(errors, get_wait_group_errors...)
			}

			get_result_group, get_result_group_errors := crud_result_group(*trace_id, nil, "read")
			if get_result_group_errors != nil {
				errors = append(errors, get_result_group_errors...)
			}

			if len(errors) > 0 {
				return nil, errors
			}

			if common.IsNil(get_result_group) && !common.IsNil(get_wait_group) {
				get_wait_group.Wait()
				get_result_group, get_result_group_errors = crud_result_group(*trace_id, nil, "read")
				if get_result_group_errors != nil {
					errors = append(errors, get_result_group_errors...)
				} else if common.IsNil(get_result_group) {
					errors = append(errors, fmt.Errorf("result group is nil"))
				}
			}

			if len(errors) > 0 {
				crud_result_group(*trace_id, nil, "delete")
				crud_wait_group(*trace_id, nil, "delete")
				return nil, errors
			}
			
			request = *get_result_group
			crud_result_group(*trace_id, nil, "delete")
			crud_wait_group(*trace_id, nil, "delete")	
		} 
		return &request, nil
	}

	process_request_function := func(w http.ResponseWriter, req *http.Request) {
		var process_request_errors []error
		
		if !(req.Method == "POST" || req.Method == "PATCH" || req.Method == "PUT") {
			process_request_errors = append(process_request_errors, fmt.Errorf("request method not supported: " + req.Method))
		}

		if len(process_request_errors) > 0 {
			http_extension.WriteResponse(w, json.NewMap(), process_request_errors)
			return
		}

		body_payload, body_payload_error := ioutil.ReadAll(req.Body)
		if body_payload_error != nil {
			process_request_errors = append(process_request_errors, body_payload_error)
		}

		if len(process_request_errors) > 0 {
			http_extension.WriteResponse(w, json.NewMap(), process_request_errors)
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
			http_extension.WriteResponse(w, json.NewMap(), process_request_errors)
			return
		}

		queue, queue_errors := request.GetString("[queue]")
		if queue_errors != nil {
			process_request_errors = append(process_request_errors, queue_errors...)
		} else if common.IsNil(queue) {
			process_request_errors = append(process_request_errors, fmt.Errorf("[queue] is nil"))
		}

		if len(process_request_errors) > 0 {
			http_extension.WriteResponse(w, json.NewMap(), process_request_errors)
			return
		}

		trace_id, trace_id_errors := request.GetString("[trace_id]")
		if trace_id_errors != nil {
			process_request_errors = append(process_request_errors, trace_id_errors...)
		} else if common.IsNil(trace_id) {
			temp_trace_id := common.GenerateTraceId(incrementMessageCount(), queue_name)
			trace_id = &temp_trace_id
			request.SetString("[trace_id]", &temp_trace_id)
		}

		if *queue == "" {
			process_request_errors = append(process_request_errors, fmt.Errorf("[queue] has empty value"))
		}

		if len(process_request_errors) > 0 {
			http_extension.WriteResponse(w, json.NewMap(), process_request_errors)
			return
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
			http_extension.WriteResponse(w, request, process_request_errors)
			return
		}
		
		if *queue_mode == "PushBack" {
			push_back_response, push_back_response_errors := push_back_process_request(*request)
			if push_back_response_errors != nil {
				process_request_errors = append(process_request_errors, push_back_response_errors...)
			} else if common.IsNil(push_back_response) {
				process_request_errors = append(process_request_errors, fmt.Errorf("push_back_response is nil"))
			} else {
				request = push_back_response
			}
			
		} else if *queue_mode == "GetAndRemoveFront" {
			next_message, next_message_errors := get_next_message_from_queue(*trace_id)
			if next_message_errors != nil {
				process_request_errors = append(process_request_errors, next_message_errors...)
			} else if common.IsNil(next_message) {
				process_request_errors = append(process_request_errors, fmt.Errorf("next_message is nil"))
			} else {
				request = &next_message
			}
		} else if *queue_mode == "complete" {
			complete_request(*request)
		} else {
			process_request_errors = append(process_request_errors, fmt.Errorf("[queue_mode] not supported please implement: %s", *queue_mode))
		}

		
		cloned_request, cloned_request_errors := request.Clone()
		if cloned_request_errors != nil {
			process_request_errors = append(process_request_errors, cloned_request_errors...)
		} else if common.IsNil(cloned_request) {
			process_request_errors = append(process_request_errors, fmt.Errorf("cloned request is nil"))
		}

		http_extension.WriteResponse(w, cloned_request, process_request_errors)
	}

	x := QueueController{
		SetWakeupProcessorManagerFunction: func(function *func()) {
			temp_function := function
			processor_callback_function = temp_function
		},
		GetCompleteFunction: func() (*func(json.Map) []error) {
			function := complete_request
			return &function
		},
		GetNextMessageFunction: func() (*func(string) (json.Map,[]error)) {
			function := get_next_message_from_queue
			return &function
		},
		GetPushBackFunction: func() (*func(json.Map) (*json.Map, []error)) {
			function := push_back_process_request
			return &function
		},
		GetProcessRequestFunction: func() *func(w http.ResponseWriter, req *http.Request) {
			function := process_request_function
			return &function
		},
	}
	//set_queue_controller(&x)

	if len(errors) > 0 {
		return nil, errors
	}

	return &x, nil
}
