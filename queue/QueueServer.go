package queue

import (
	"fmt"
	"net/http"
	dao "github.com/matehaxor03/holistic_db_client/dao"
	common "github.com/matehaxor03/holistic_common/common"
	"sync"
)

type QueueServer struct {
	Start func() []error
	SetProcessorWakeUpFunctions func(map[string](*func()))
	GetControllers func() (map[string](*QueueController))
	GetControllerByName func(controller_name string) (*QueueController, error)
	GetControllerNames func() []string
}

func NewQueueServer(port string, server_crt_path string, server_key_path string, processor_domain_name string, processor_port string) (*QueueServer, []error) {
	var errors []error
	var processor_wakeup_functions map[string](*func())
	lock_processor_wakeup_function := &sync.Mutex{}
	get_controller_by_name_lock := &sync.RWMutex{}

	set_processor_wakeup_functions := func(functions  map[string](*func())) {
		processor_wakeup_functions = functions
	}

	get_processor_wakeup_functions := func(queue_name string) *func() {
		lock_processor_wakeup_function.Lock()
		defer lock_processor_wakeup_function.Unlock()
		if processor_wakeup_functions == nil {
			return nil
		}
		processor_wakeup_function, found := processor_wakeup_functions[queue_name]
		if !found {
			return nil
		}
		return processor_wakeup_function
	}

	client_manager, client_manager_errors := dao.NewClientManager()
	if client_manager_errors != nil {
		return nil, client_manager_errors
	}

	test_read_client, test_read_client_errors := client_manager.GetClient("127.0.0.1", "3306", "holistic", "holistic_r")
	if test_read_client_errors != nil {
		return nil, test_read_client_errors
	}
	
	test_read_database := test_read_client.GetDatabase()

	controllers := make(map[string](*QueueController))
	table_names, table_names_errors := test_read_database.GetTableNames()
	if table_names_errors != nil {
		return nil, table_names_errors
	}

	controllers["Run_Sync"], _ = NewQueueController("Run_Sync", processor_domain_name, processor_port)


	for _, table_name := range table_names {
		controllers["CreateRecords_"+table_name], _ = NewQueueController("CreateRecords_"+table_name, processor_domain_name, processor_port)
		controllers["CreateRecord_"+table_name], _ = NewQueueController("CreateRecord_"+table_name, processor_domain_name, processor_port)
		controllers["ReadRecords_"+table_name], _ = NewQueueController("ReadRecords_"+table_name, processor_domain_name, processor_port)
		controllers["UpdateRecords_"+table_name], _ = NewQueueController("UpdateRecords_"+table_name, processor_domain_name, processor_port)
		controllers["UpdateRecord_"+table_name], _ = NewQueueController("UpdateRecord_"+table_name, processor_domain_name, processor_port)
		controllers["CreateRecords_"+table_name], _ = NewQueueController("CreateRecords_"+table_name, processor_domain_name, processor_port)
		controllers["DeleteRecords_"+table_name], _ = NewQueueController("DeleteRecords_"+table_name, processor_domain_name, processor_port)
		controllers["GetSchema_"+table_name], _ = NewQueueController("GetSchema_"+table_name, processor_domain_name, processor_port)
		controllers["GetTableCount_"+table_name], _ = NewQueueController("GetTableCount_"+table_name, processor_domain_name, processor_port)
	}

	controllers["Run_StartBranchInstance"], _ = NewQueueController("Run_StartBranchInstance", processor_domain_name, processor_port)
	controllers["Run_NotStarted"], _ = NewQueueController("Run_NotStarted", processor_domain_name, processor_port)
	controllers["Run_Start"], _ = NewQueueController("Run_Start", processor_domain_name, processor_port)
	controllers["Run_CreateSourceFolder"], _ = NewQueueController("Run_CreateSourceFolder", processor_domain_name, processor_port)
	controllers["Run_CreateDomainNameFolder"], _ = NewQueueController("Run_CreateDomainNameFolder", processor_domain_name, processor_port)
	controllers["Run_CreateRepositoryAccountFolder"], _ = NewQueueController("Run_CreateRepositoryAccountFolder", processor_domain_name, processor_port)
	controllers["Run_CreateRepositoryFolder"], _ = NewQueueController("Run_CreateRepositoryFolder", processor_domain_name, processor_port)
	controllers["Run_CreateBranchesFolder"], _ = NewQueueController("Run_CreateBranchesFolder", processor_domain_name, processor_port)
	controllers["Run_CreateTagsFolder"], _ = NewQueueController("Run_CreateTagsFolder", processor_domain_name, processor_port)
	controllers["Run_CreateBranchInstancesFolder"], _ = NewQueueController("Run_CreateBranchInstancesFolder", processor_domain_name, processor_port)
	controllers["Run_CreateTagInstancesFolder"], _ = NewQueueController("Run_CreateTagInstancesFolder", processor_domain_name, processor_port)
	controllers["Run_CreateBranchOrTagFolder"], _ = NewQueueController("Run_CreateBranchOrTagFolder", processor_domain_name, processor_port)
	controllers["Run_CloneBranchOrTagFolder"], _ = NewQueueController("Run_CloneBranchOrTagFolder", processor_domain_name, processor_port)
	controllers["Run_PullLatestBranchOrTagFolder"], _ = NewQueueController("Run_PullLatestBranchOrTagFolder", processor_domain_name, processor_port)
	controllers["Run_CreateInstanceFolder"], _ = NewQueueController("Run_CreateInstanceFolder", processor_domain_name, processor_port)
	controllers["Run_CopyToInstanceFolder"], _ = NewQueueController("Run_CopyToInstanceFolder", processor_domain_name, processor_port)
	controllers["Run_Clean"], _ = NewQueueController("Run_Clean", processor_domain_name, processor_port)
	controllers["Run_Lint"], _ = NewQueueController("Run_Lint", processor_domain_name, processor_port)
	controllers["Run_Build"], _ = NewQueueController("Run_Build", processor_domain_name, processor_port)
	controllers["Run_UnitTests"], _ = NewQueueController("Run_UnitTests", processor_domain_name, processor_port)
	controllers["Run_IntegrationTests"], _ = NewQueueController("Run_IntegrationTests", processor_domain_name, processor_port)
	controllers["Run_IntegrationTestSuite"], _ = NewQueueController("Run_IntegrationTestSuite", processor_domain_name, processor_port)
	controllers["Run_DeleteInstanceFolder"], _ = NewQueueController("Run_DeleteInstanceFolder", processor_domain_name, processor_port)
	controllers["Run_End"], _ = NewQueueController("Run_End", processor_domain_name, processor_port)

	controllers["GetTableNames"], _ = NewQueueController("GetTableNames", processor_domain_name, processor_port)

	validate := func() []error {
		return nil
	}

	get_controller_by_name := func(contoller_name string) (*QueueController, error) {
		get_controller_by_name_lock.Lock()
		defer get_controller_by_name_lock.Unlock()
		queue_obj, queue_found := controllers[contoller_name]
		if !queue_found {
			return nil, fmt.Errorf("queue not found %s", contoller_name)
		} else if common.IsNil(queue_obj) {
			return nil, fmt.Errorf("queue found but is nil %s", contoller_name)
		} 
		return queue_obj, nil
	}
	
	get_controller_names := func() []string {
		get_controller_by_name_lock.Lock()
		defer get_controller_by_name_lock.Unlock()
		controller_names := make([]string, len(controllers))
		for key, _ := range controllers {
			controller_names = append(controller_names, key)
		}
		return controller_names
	}

	getPort := func() string {
		return port
	}

	getServerCrtPath := func() string {
		return server_crt_path
	}

	getServerKeyPath := func() string {
		return server_key_path
	}

	x := QueueServer{
		SetProcessorWakeUpFunctions: func(functions map[string](*func())) {
			set_processor_wakeup_functions(functions)
		},
		Start: func() []error {
			var start_server_errors []error
			temp_controller_names := get_controller_names()

			for _, temp_controller_name := range temp_controller_names {
				temp_controller, temp_controller_errors := get_controller_by_name(temp_controller_name)
				if temp_controller_errors != nil {
					start_server_errors = append(start_server_errors, temp_controller_errors)
					continue
				} else if common.IsNil(temp_controller) {
					start_server_errors = append(start_server_errors, fmt.Errorf("controller is nil: %s", temp_controller))
					continue
				}
				fmt.Println("adding controller: " + "/queue_api/" + temp_controller_name)
				temp_controller.SetWakeupProcessorManagerFunction(get_processor_wakeup_functions(temp_controller_name))
				http.HandleFunc("/queue_api/" + temp_controller_name, *(temp_controller.GetProcessRequestFunction()))
			}

			err := http.ListenAndServeTLS(":"+ getPort(),  getServerCrtPath(), getServerKeyPath(), nil)
			if err != nil {
				start_server_errors = append(start_server_errors, err)
			}

			if len(start_server_errors) > 0 {
				return start_server_errors
			}

			return nil
		},
		GetControllers: func() (map[string](*QueueController)) {
			return controllers
		},
		GetControllerByName: func(controller_name string) (*QueueController, error) {
			return get_controller_by_name(controller_name)
		},
		GetControllerNames: func() []string {
			return get_controller_names()
		},
	}

	validate_errors := validate()
	if validate_errors != nil {
		errors = append(errors, validate_errors...)
	}

	if len(errors) > 0 {
		return nil, errors
	}

	return &x, nil
}
