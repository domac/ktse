package core

//任务类型
const (
	ScriptTask    = 1
	RpcTaskGET    = 2
	RpcTaskPOST   = 3
	RpcTaskPUT    = 4
	RpcTaskDELETE = 5
)

//任务请求对象
type TaskRequest struct {
	Uuid         string `json:"uuid"`
	BinName      string `json:"bin_name"`
	Args         string `json:"args"` //空格分隔各个参数
	StartTime    int64  `json:"start_time,string"`
	TimeInterval string `json:"time_interval"` //空格分隔各个参数
	Index        int    `json:"index,string"`
	MaxRunTime   int64  `json:"max_run_time,string"`
	TaskType     int    `json:"task_type,string"`
}

//任务结果对象
type TaskResult struct {
	TaskRequest
	IsSuccess int64  `json:"is_success"`
	Result    string `json:"result"`
}

//任务回执
type Reply struct {
	IsResultExist int    `json:"is_result_exist"`
	IsSuccess     int    `json:"is_success"`
	Result        string `json:"message"`
}