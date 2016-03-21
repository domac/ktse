package core

import (
	"github.com/labstack/echo"
	mw "github.com/labstack/echo/middleware"
	"github.com/pborman/uuid"
	"github.com/phillihq/ktse/logger"
	"net/http"
	"strconv"
)

//注册中间件
func (b *Broker) RegisterMiddleware() {
	b.web.Use(mw.Logger())
	b.web.Use(mw.Recover())
}

//注册Rest地址
func (b *Broker) RegisterURL() {
	b.web.Post("/api/task/script", echo.HandlerFunc(b.CreateScriptTaskRequest))
	b.web.Post("/api/task/rpc", echo.HandlerFunc(b.CreateRpcTaskRequest))
	b.web.Get("/api/task/result", echo.HandlerFunc(b.GetTaskResult))
	b.web.Get("/api/task/count/undo", echo.HandlerFunc(b.UndoTaskCount))
	b.web.Get("/api/task/result/failure/:date", echo.HandlerFunc(b.FailTaskCount))
	b.web.Get("/api/task/result/success/:date", echo.HandlerFunc(b.SuccessTaskCount))
}

//提交脚本任务请求
func (b *Broker) CreateScriptTaskRequest(c echo.Context) error {
	startTime, _ := strconv.ParseInt(c.Query("start_time"), 10, 64)
	maxRunTime, _ := strconv.ParseInt(c.Query("max_run_time"), 10, 64)
	args := struct {
		BinName      string `json:"bin_name"`
		Args         string `json:"args"` //空格分隔各个参数
		StartTime    int64  `json:"start_time,string"`
		TimeInterval string `json:"time_interval"` //空格分隔各个参数
		MaxRunTime   int64  `json:"max_run_time,string"`
	}{
		BinName:      c.Query("bin_name"),
		Args:         c.Query("args"),
		StartTime:    startTime,
		TimeInterval: c.Query("time_interval"),
		MaxRunTime:   maxRunTime,
	}

	taskRequest := new(TaskRequest)
	taskRequest.Uuid = uuid.New()
	//判断脚本路径
	if len(args.BinName) == 0 {
		return c.JSON(http.StatusForbidden, ErrInvalidArgument)
	}
	taskRequest.BinName = args.BinName
	taskRequest.Args = args.Args
	taskRequest.StartTime = args.StartTime
	taskRequest.Index = 0
	taskRequest.TimeInterval = args.TimeInterval
	taskRequest.MaxRunTime = args.MaxRunTime
	taskRequest.TaskType = ScriptTask

	//交给broker处理请求
	err := b.HandleRequest(taskRequest)
	if err != nil {
		return c.JSON(http.StatusForbidden, err.Error())
	}
	//日志输出
	logger.GetLogger().Infoln("Broker", "CreateScriptTaskRequest", "ok", 0,
		"uuid", taskRequest.Uuid,
		"bin_name", taskRequest.BinName,
		"args", taskRequest.Args,
		"start_time", taskRequest.StartTime,
		"time_interval", taskRequest.TimeInterval,
		"index", taskRequest.Index,
		"max_run_time", taskRequest.MaxRunTime,
		"task_type", taskRequest.TaskType,
	)

	return c.JSON(http.StatusOK, taskRequest.Uuid)
}

//提交http方式的任务请求
func (b *Broker) CreateRpcTaskRequest(c echo.Context) error {
	startTime, _ := strconv.ParseInt(c.Query("start_time"), 10, 64)
	maxRunTime, _ := strconv.ParseInt(c.Query("max_run_time"), 10, 64)
	args := struct {
		Method       string `json:"method"`
		URL          string `json:"url"`
		Args         string `json:"args"` //json Marshal后的字符串
		StartTime    int64  `json:"start_time,string"`
		TimeInterval string `json:"time_interval"` //空格分隔各个参数
		MaxRunTime   int64  `json:"max_run_time,string"`
	}{
		Method:       c.Query("method"),
		URL:          c.Query("url"),
		Args:         c.Query("args"),
		StartTime:    startTime,
		TimeInterval: c.Query("time_interval"),
		MaxRunTime:   maxRunTime,
	}

	taskRequest := new(TaskRequest)
	taskRequest.Uuid = uuid.New()
	if len(args.URL) == 0 {
		return c.JSON(http.StatusForbidden, ErrInvalidArgument)
	}

	taskRequest.BinName = args.URL
	taskRequest.Args = args.Args
	taskRequest.StartTime = args.StartTime
	taskRequest.TimeInterval = args.TimeInterval
	taskRequest.Index = 0
	taskRequest.MaxRunTime = args.MaxRunTime

	switch args.Method {
	case "GET":
		taskRequest.TaskType = RpcTaskGET
	case "POST":
		taskRequest.TaskType = RpcTaskPOST
	case "PUT":
		taskRequest.TaskType = RpcTaskPUT
	case "DELETE":
		taskRequest.TaskType = RpcTaskDELETE
	default:
		return c.JSON(http.StatusForbidden, ErrInvalidArgument.Error())
	}

	err := b.HandleRequest(taskRequest)
	if err != nil {
		return c.JSON(http.StatusForbidden, err.Error())
	}
	logger.GetLogger().Infoln("Broker", "CreateRpcTaskRequest", "ok", 0,
		"uuid", taskRequest.Uuid,
		"bin_name", taskRequest.BinName,
		"args", taskRequest.Args,
		"start_time", taskRequest.StartTime,
		"time_interval", taskRequest.TimeInterval,
		"index", taskRequest.Index,
		"max_run_time", taskRequest.MaxRunTime,
		"task_type", taskRequest.TaskType,
	)
	return c.JSON(http.StatusOK, taskRequest.Uuid)
}

//获取任务结果(根据UUID)
func (b *Broker) GetTaskResult(c echo.Context) error {
	uuid := c.Query("uuid")
	logger.GetLogger().Infoln("查询uuid=", uuid)
	if len(uuid) == 0 {
		return c.JSON(http.StatusForbidden, ErrInvalidArgument.Error())
	}
	reply, err := b.HandleTaskResult(uuid)
	if err != nil {
		return c.JSON(http.StatusForbidden, err.Error())
	}
	return c.JSON(http.StatusOK, reply)
}

//获取未执行的任务数量
func (b *Broker) UndoTaskCount(c echo.Context) error {
	count, err := b.GetUndoTaskCount()
	if err != nil {
		return c.JSON(http.StatusForbidden, err.Error())
	}
	return c.JSON(http.StatusOK, count)
}

//获取失败的任务数量
func (b *Broker) FailTaskCount(c echo.Context) error {
	date := c.Param("date")
	count, err := b.GetFailTaskCount(date)
	if err != nil {
		return c.JSON(http.StatusForbidden, err.Error())
	}
	return c.JSON(http.StatusOK, count)
}

//获取成功的任务数量
func (b *Broker) SuccessTaskCount(c echo.Context) error {
	date := c.Param("date")
	count, err := b.GetSuccessTaskCount(date)
	if err != nil {
		return c.JSON(http.StatusForbidden, err.Error())
	}
	return c.JSON(http.StatusOK, count)
}
