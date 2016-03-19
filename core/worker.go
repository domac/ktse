package core

import (
	"bytes"
	"fmt"
	"gopkg.in/redis.v3"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"
	"github.com/phillihq/ktse/logger"
)

type Worker struct {
	cfg         *WorkerConfig
	redisAddr   string
	redisDB     int
	running     bool
	redisClient *redis.Client
}

func NewWorker(cfg *WorkerConfig) (*Worker, error) {
	var err error
	w := new(Worker)
	w.cfg = cfg

	vec := strings.SplitN(cfg.RedisAddr, "/", 2)
	if len(vec) == 2 {
		w.redisAddr = vec[0]
		w.redisDB, err = strconv.Atoi(vec[1])
		if err != nil {
			return nil, err
		}
	} else {
		w.redisAddr = vec[0]
		w.redisDB = DefaultRedisDB
	}

	w.redisClient = redis.NewClient(
		&redis.Options{
			Addr:     w.redisAddr,
			Password: "",
			DB:       int64(w.redisDB),
		},
	)
	_, err = w.redisClient.Ping().Result()
	if err != nil {
		logger.GetLogger().Errorln("worker", "NewWorker", "ping redis fail", 0, "err", err.Error())
		return nil, err
	}
	return w, nil
}

func (w *Worker) Run() error {
	var taskResult *TaskResult
	w.running = true
	for w.running {
		uuid, err := w.redisClient.SPop(RequestUuidSet).Result()
		if err == redis.Nil {
			time.Sleep(time.Second)
			continue
		}
		if err != nil {
			logger.GetLogger().Errorln("Worker", "run", "spop error", 0, "error", err.Error())
			continue
		}
		reqKey := fmt.Sprintf("t_%s", uuid)
		//获取请求中所有值
		request, err := w.redisClient.HMGet(reqKey,
			"uuid",
			"bin_name",
			"args",
			"start_time",
			"time_interval",
			"index",
			"max_run_time",
			"task_type",
		).Result()
		if err != nil {
			logger.GetLogger().Errorln("Worker", "run", err.Error(), 0, "req_key", reqKey)
			continue
		}

		//key不存在
		if request[0] == nil {
			logger.GetLogger().Errorln("Worker", "run", "Key is not exist", 0, "req_key", reqKey)
			continue
		}

		_, err = w.redisClient.Del(reqKey).Result()
		if err != nil {
			logger.GetLogger().Errorln("Worker", "run", "delete result failed", 0, "req_key", reqKey)
		}
		//执行请求的任务
		taskResult, err = w.DoTaskRequest(request)
		if err != nil {
			logger.GetLogger().Errorln("Worker", "run", "DoTaskRequest", 0, "err", err.Error(),
				"req_key", reqKey, "bin_name", request[1], "task_type", request[7])
		} else {
			w.SetSuccessTaskCount(reqKey)
		}

		if taskResult != nil {
			err = w.SetTaskResult(taskResult)
			if err != nil {
				logger.GetLogger().Errorln("Worker", "run", "DoScrpitTaskRequest", 0,
					"err", err.Error(), "req_key", reqKey)
			}
			logger.GetLogger().Infoln("worker", "run", "do task success", 0, "req_key", reqKey,
				"result", taskResult.Result)
		}
		if w.cfg.Peroid != 0 {
			time.Sleep(time.Second * time.Duration(w.cfg.Peroid))
		}
	}
	return nil
}

func (w *Worker) Close() {
	w.running = false
	w.redisClient.Close()
}

func (w *Worker) DoTaskRequest(args []interface{}) (*TaskResult, error) {
	var err error
	var output string
	req := new(TaskRequest)
	ret := new(TaskResult)

	req.Uuid = args[0].(string)
	req.BinName = args[1].(string)
	req.Args = args[2].(string)
	req.StartTime, err = strconv.ParseInt(args[3].(string), 10, 64)
	if err != nil {
		return nil, err
	}
	req.TimeInterval = args[4].(string)
	req.Index, err = strconv.Atoi(args[5].(string))
	if err != nil {
		return nil, err
	}
	req.MaxRunTime, err = strconv.ParseInt(args[6].(string), 10, 64)
	if err != nil {
		return nil, err
	}
	req.TaskType, err = strconv.Atoi(args[7].(string))
	if err != nil {
		return nil, err
	}

	switch req.TaskType {
	case ScriptTask:
		//执行脚本请求
		output, err = w.DoScriptTaskRequest(req)
	case RpcTaskGET, RpcTaskPOST, RpcTaskPUT, RpcTaskDELETE:
		//执行RPC请求
		output, err = w.DoRpcTaskRequest(req)
	default:
		err = ErrInvalidArgument
		logger.GetLogger().Errorln("Worker", "DoTaskRequest", "task type error", 0, "task_type", req.TaskType)
	}

	ret.TaskRequest = *req
	if err != nil {
		ret.IsSuccess = int64(0)
		ret.Result = err.Error()
		return ret, nil
	}
	ret.IsSuccess = int64(1)
	ret.Result = output

	return ret, nil
}

//执行脚本请求
func (w *Worker) DoScriptTaskRequest(req *TaskRequest) (string, error) {
	var output string
	var err error
	var maxRunTime int64

	binPath := path.Clean(w.cfg.BinPath + "/" + req.BinName)
	_, err = os.Stat(binPath)
	if err != nil && os.IsNotExist(err) {
		logger.GetLogger().Errorln("worker", "DoScrpitTaskRequest", "File not exist", 0,
			"key", fmt.Sprintf("t_%s", req.Uuid),
			"bin_path", binPath,
		)
		return "", ErrFileNotExist
	}

	if req.MaxRunTime == 0 {
		maxRunTime = w.cfg.TaskRunTime
	} else {
		maxRunTime = req.MaxRunTime
	}

	if len(req.Args) == 0 {
		output, err = w.ExecBin(binPath, nil, maxRunTime)
	} else {
		argsVec := strings.Split(req.Args, " ")
		output, err = w.ExecBin(binPath, argsVec, maxRunTime)
	}
	return output, err
}

//命令执行函数
func (w *Worker) ExecBin(binPath string, args []string, maxRunTime int64) (string, error) {
	var cmd *exec.Cmd
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	var err error

	if len(args) == 0 {
		cmd = exec.Command(binPath)
	} else {
		cmd = exec.Command(binPath, args...)
	}

	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Start()
	err, _ = w.CmdRunWithTimeout(cmd, time.Duration(maxRunTime)*time.Second)
	if err != nil {
		return "", err
	}

	if len(stderr.String()) != 0 {
		errMsg := strings.TrimRight(stderr.String(), "\n")
		return "", NewError(errMsg)
	}
	return strings.TrimRight(stdout.String(), "\n"), nil
}

func (w *Worker) CmdRunWithTimeout(cmd *exec.Cmd, timeout time.Duration) (error, bool) {
	var err error

	errCh := make(chan error)

	go func() {
		errCh <- cmd.Wait()
	}()

	select {
	case <-time.After(timeout):
		if err = cmd.Process.Kill(); err != nil {
			logger.GetLogger().Errorln("worker", "CmdRunTimeout", "kill error", 0, "path", cmd.Path, "error", err.Error())
		}
		logger.GetLogger().Infoln("worker", "CmdRunWithTimeout", "kill process", 0, "path", cmd.Path, "error", ErrExecTimeout.Error())
		go func() {
			<-errCh
		}()
		return ErrExecTimeout, true
	case err = <-errCh:
		return err, false
	}
}

//执行RPC任务请求
func (w *Worker) DoRpcTaskRequest(req *TaskRequest) (string, error) {
	var method string
	switch req.TaskType {
	case RpcTaskGET:
		method = "GET"
	case RpcTaskPOST:
		method = "POST"
	case RpcTaskPUT:
		method = "PUT"
	case RpcTaskDELETE:
		method = "DELETE"
	default:
		method = "GET"
	}
	url := req.BinName
	args := req.Args
	request, err := w.newHttpRequest(method, url, args)
	if err != nil {
		return "", err
	}
	result, err := w.callRpc(request, time.Second*time.Duration(req.MaxRunTime))
	return result, err
}

//调用HTTP请求
func (w *Worker) callRpc(req *http.Request, maxRunTime time.Duration) (string, error) {
	var timeout time.Duration
	if w.cfg.TaskRunTime != 0 {
		timeout = time.Duration(w.cfg.TaskRunTime) * time.Second
	} else {
		timeout = maxRunTime
	}
	//new a http client with timeout setting
	client := &http.Client{
		Timeout: timeout,
	}
	r, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer r.Body.Close()
	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return "", err
	}
	if r.StatusCode != http.StatusOK {
		return "", NewError(string(buf))
	}

	return string(buf), nil
}

//创建HTTP连接
func (w *Worker) newHttpRequest(method string, url string, args string) (*http.Request, error) {

	var body io.Reader
	if len(args) != 0 {
		body = bytes.NewBuffer([]byte(args))
	}
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

//设置任务执行结果
func (w *Worker) SetTaskResult(result *TaskResult) error {
	key := fmt.Sprintf("r_%s", result.Uuid)
	setCmd := w.redisClient.HMSet(key,
		"uuid", result.Uuid,
		"bin_name", result.BinName,
		"args", result.Args,
		"start_time", strconv.FormatInt(result.StartTime, 10),
		"time_interval", result.TimeInterval,
		"index", strconv.Itoa(result.Index),
		"max_run_time", strconv.FormatInt(result.MaxRunTime, 10),
		"task_type", strconv.Itoa(result.TaskType),
		"is_success", strconv.Itoa(int(result.IsSuccess)),
		"result", result.Result,
	)
	err := setCmd.Err()
	if err != nil {
		return err
	}
	//如果任务是执行失败
	if result.IsSuccess == int64(0) {
		saddCmd := w.redisClient.SAdd(FailResultUuidSet, result.Uuid)
		err = saddCmd.Err()
		if err != nil {
			return err
		}
	}

	//设置结果的过期时间
	_, err = w.redisClient.Expire(key, time.Second*time.Duration(w.cfg.ResultKeepTime)).Result()
	if err != nil {
		return err
	}
	return nil
}

//设置成功的任务记录
func (w *Worker) SetSuccessTaskCount(reqKey string) error {
	successTaskKey := fmt.Sprintf(SuccessTaskKey, time.Now().Format(TimeFormat))
	count, err := w.redisClient.Incr(successTaskKey).Result()
	if err != nil {
		logger.GetLogger().Errorln("Worker", "SetSuccessTaskCount", "Incr", 0, "err", err.Error(),
			"req_key", reqKey)
		return err
	}
	//第一次设置该key
	if count == 1 {
		//保存一个月
		expireTime := time.Second * time.Duration(60*60*24*30)
		_, err = w.redisClient.Expire(successTaskKey, expireTime).Result()
		if err != nil {
			logger.GetLogger().Errorln("Worker", "SetSuccessTaskCount", "Expire", 0, "err", err.Error(),
				"req_key", reqKey)
			return err
		}
	}
	return nil
}
