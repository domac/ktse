package core

import (
	"fmt"
	"github.com/labstack/echo"
	"github.com/labstack/echo/engine/standard"
	"github.com/phillihq/ktse/logger"
	"gopkg.in/redis.v3"
	"strconv"
	"strings"
	"time"
)

type Broker struct {
	cfg                *BrokerConfig
	port               string
	redisAddr          string
	redisDB            int
	running            bool
	web                *echo.Echo
	redisClient        *redis.Client
	redisClusterClient *redis.ClusterClient
	timer              *Timer
	cluster            bool
}

func NewBroker(cfg *BrokerConfig, cluster bool) (*Broker, error) {
	var err error
	broker := new(Broker)
	broker.cfg = cfg
	broker.port = cfg.Port

	if len(broker.port) == 0 {
		return nil, ErrInvalidArgument
	}

	vec := strings.SplitN(cfg.RedisAddr, "/", 2)
	if len(vec) == 2 {
		broker.redisAddr = vec[0]
		broker.redisDB, err = strconv.Atoi(vec[1])
		if err != nil {
			return nil, err
		}
	} else {
		broker.redisAddr = vec[0]
		broker.redisDB = DefaultRedisDB
	}
	broker.web = echo.New()
	broker.timer = NewT(time.Millisecond * 10)
	go broker.timer.Start()

	broker.redisClient = redis.NewClient(
		&redis.Options{
			Addr:     broker.redisAddr,
			Password: "",
			DB:       int64(broker.redisDB),
		},
	)

	broker.cluster = cluster
	broker.redisClusterClient = redis.NewClusterClient(
		&redis.ClusterOptions{
			Addrs: []string{broker.redisAddr},
		},
	)

	if broker.IsCluster() {
		_, err = broker.redisClusterClient.Ping().Result()
	} else {
		_, err = broker.redisClient.Ping().Result()
	}
	if err != nil {
		logger.GetLogger().Errorln("broker", "NewBroker", "ping redis fail", 0, "err", err.Error())
		return nil, err
	}
	return broker, err
}

func (b *Broker) Run() {
	b.running = true
	b.RegisterMiddleware()
	b.RegisterURL()
	go b.HandleFailTask()
	b.web.Run(standard.New(b.cfg.Port))
}

func (b *Broker) Close() {
	b.running = false
	b.redisClient.Close()
	b.redisClusterClient.Close()
	b.timer.Stop()
}

//是否采用集群模式
func (b *Broker) IsCluster() bool {
	return b.cluster
}

//处理任务结果
func (b *Broker) HandleTaskResult(uuid string) (*Reply, error) {
	if len(uuid) == 0 {
		return nil, ErrInvalidArgument
	}
	key := fmt.Sprintf("r_%s", uuid)
	var result []interface{}
	var err error
	if b.IsCluster() {
		result, err = b.redisClusterClient.HMGet(key,
			"is_success",
			"result",
		).Result()
	} else {
		result, err = b.redisClient.HMGet(key,
			"is_success",
			"result",
		).Result()
	}
	if err != nil {
		logger.GetLogger().Errorln("Broker", "HandleTaskResult", err.Error(), 0, "req_key", key)
		return nil, err
	}
	//key不存在
	if result[0] == nil {
		return nil, ErrResultNotExist
	}
	isSuccess, err := strconv.Atoi(result[0].(string))
	if err != nil {
		return nil, err
	}
	ret := result[1].(string)
	return &Reply{
		IsResultExist: 1,
		IsSuccess:     isSuccess,
		Result:        ret,
	}, nil
}

//处理请求
func (b *Broker) HandleRequest(request *TaskRequest) error {
	var err error
	now := time.Now().Unix()
	if request.StartTime == 0 {
		request.StartTime = now
	}
	if request.StartTime <= now {
		err = b.AddRequestToRedis(request) //把任务信息添加到redis
		if err != nil {
			return err
		}
	} else {
		afterTime := time.Second * time.Duration(request.StartTime-now)
		//根据调度时间,把任务信息添加到redis
		b.timer.NewTimer(afterTime, b.AddRequestToRedis, request)
	}
	return nil
}

//处理失败的任务
func (b *Broker) HandleFailTask() error {
	var uuid string
	var err error

	for b.running {

		if b.IsCluster() {
			uuid, err = b.redisClusterClient.SPop(FailResultUuidSet).Result()
		} else {
			uuid, err = b.redisClient.SPop(FailResultUuidSet).Result()
		}
		if err == redis.Nil {
			time.Sleep(time.Second)
			continue
		}
		if err != nil {
			logger.GetLogger().Errorln("Broker", "HandleFailTask", "spop error", 0, "error", err.Error())
			continue
		}

		key := fmt.Sprintf("r_%s", uuid)

		var timeInterval string
		var err error

		if b.IsCluster() {
			timeInterval, err = b.redisClusterClient.HGet(key, "time_interval").Result()
		} else {
			timeInterval, err = b.redisClient.HGet(key, "time_interval").Result()
		}

		if err != nil {
			logger.GetLogger().Errorln("Broker", "HandleFailTask", err.Error(), 0, "key", key)
			continue
		}

		//没有超时重试机制
		if len(timeInterval) == 0 {
			b.SetFailTaskCount(fmt.Sprintf("t_%s", uuid))
			continue
		}

		var results []interface{}

		if b.IsCluster() {
			results, err = b.redisClusterClient.HMGet(key,
				"uuid",
				"bin_name",
				"args",
				"start_time",
				"time_interval",
				"index",
				"max_run_time",
				"task_type").Result()
		} else {
			results, err = b.redisClient.HMGet(key,
				"uuid",
				"bin_name",
				"args",
				"start_time",
				"time_interval",
				"index",
				"max_run_time",
				"task_type").Result()
		}
		if err != nil {
			logger.GetLogger().Errorln("Broker", "HandleFailTask", err.Error(), 0, "key", key)
			continue
		}
		//key已经过期
		if results[0] == nil {
			logger.GetLogger().Errorln("Broker", "HandleFailTask", "result expired", 0, "key", key)
			continue
		}

		//删除结果
		if b.IsCluster() {
			_, err = b.redisClusterClient.Del(key).Result()
		} else {
			_, err = b.redisClient.Del(key).Result()
		}

		if err != nil {
			logger.GetLogger().Errorln("Broker", "HandleFailTask", "delete result failed", 0, "key", key)
		}
		err = b.resetTaskRequest(results)
		if err != nil {
			logger.GetLogger().Errorln("Broker", "HandleFailTask", err.Error(), 0, "key", key)
			b.SetFailTaskCount(fmt.Sprintf("t_%s", uuid))
		}
	}
	return nil
}

func (b *Broker) SetFailTaskCount(reqKey string) error {
	failTaskKey := fmt.Sprintf(FailTaskKey, time.Now().Format(TimeFormat))

	var count int64
	var err error

	if b.IsCluster() {
		count, err = b.redisClusterClient.Incr(failTaskKey).Result()
	} else {
		count, err = b.redisClient.Incr(failTaskKey).Result()
	}
	if err != nil {
		logger.GetLogger().Errorln("Worker", "SetFailTaskCount", "Incr", 0, "err", err.Error(), "req_key", reqKey)
		return err
	}

	//第一次设置该key
	if count == 1 {
		expireTime := time.Second * time.Duration(60*60*24*30)
		if b.IsCluster() {
			_, err = b.redisClusterClient.Expire(failTaskKey, expireTime).Result()
		} else {
			_, err = b.redisClient.Expire(failTaskKey, expireTime).Result()
		}
		if err != nil {
			logger.GetLogger().Errorln("Worker", "SetFailTaskCount", "Expire", 0, "err", err.Error(), "req_key", reqKey)
			return err
		}
	}
	return nil
}

//把任务添加到队列中
func (b *Broker) resetTaskRequest(args []interface{}) error {
	var err error
	if len(args) == 0 || len(args) != TaskRequestItemCount {
		return ErrInvalidArgument
	}
	request := new(TaskRequest)
	request.Uuid = args[0].(string)
	request.BinName = args[1].(string)
	request.Args = args[2].(string)
	request.StartTime, err = strconv.ParseInt(args[3].(string), 10, 64)
	if err != nil {
		return err
	}
	request.TimeInterval = args[4].(string)
	request.Index, err = strconv.Atoi(args[5].(string))
	if err != nil {
		return err
	}
	request.MaxRunTime, err = strconv.ParseInt(args[6].(string), 10, 64)
	if err != nil {
		return err
	}
	request.TaskType, err = strconv.Atoi(args[7].(string))
	if err != nil {
		return err
	}
	vec := strings.Split(request.TimeInterval, " ")
	request.Index++
	if request.Index < len(vec) {
		timeLater, err := strconv.Atoi(vec[request.Index])
		if err != nil {
			return err
		}
		afterTime := time.Second * time.Duration(timeLater)
		b.timer.NewTimer(afterTime, b.AddRequestToRedis, request)
	} else {
		logger.GetLogger().Errorln("Broker", "HandleFailTask", "retry max time", 0, "key", fmt.Sprintf("t_%s", request.Uuid))
		return ErrTryMaxTimes
	}
	return nil
}

func (b *Broker) AddRequestToRedis(tr interface{}) error {
	r, ok := tr.(*TaskRequest)
	if !ok {
		return ErrInvalidArgument
	}
	key := fmt.Sprintf("t_%s", r.Uuid)

	var err error
	if b.IsCluster() {
		setCmd := b.redisClusterClient.HMSet(key,
			"uuid", r.Uuid,
			"bin_name", r.BinName,
			"args", r.Args,
			"start_time", strconv.FormatInt(r.StartTime, 10),
			"time_interval", r.TimeInterval,
			"index", strconv.Itoa(r.Index),
			"max_run_time", strconv.FormatInt(r.MaxRunTime, 10),
			"task_type", strconv.Itoa(r.TaskType),
		)
		err = setCmd.Err()
	} else {
		setCmd := b.redisClient.HMSet(key,
			"uuid", r.Uuid,
			"bin_name", r.BinName,
			"args", r.Args,
			"start_time", strconv.FormatInt(r.StartTime, 10),
			"time_interval", r.TimeInterval,
			"index", strconv.Itoa(r.Index),
			"max_run_time", strconv.FormatInt(r.MaxRunTime, 10),
			"task_type", strconv.Itoa(r.TaskType),
		)
		err = setCmd.Err()
	}

	if err != nil {
		logger.GetLogger().Errorln("Broker", "AddRequestToRedis", "HMSET error", 0,
			"set", RequestUuidSet,
			"uuid", r.Uuid,
			"err", err.Error(),
		)
		return err
	}
	if b.IsCluster() {
		saddCmd := b.redisClusterClient.SAdd(RequestUuidSet, r.Uuid)
		err = saddCmd.Err()
	} else {
		saddCmd := b.redisClient.SAdd(RequestUuidSet, r.Uuid)
		err = saddCmd.Err()
	}

	if err != nil {
		logger.GetLogger().Errorln("Broker", "AddRequestToRedis", "SADD error", 0,
			"set", RequestUuidSet,
			"uuid", r.Uuid,
			"err", err.Error(),
		)
		return err
	}
	return nil
}

//获取未执行的任务数量
func (b *Broker) GetUndoTaskCount() (int64, error) {

	var count int64
	var err error

	if b.IsCluster() {
		count, err = b.redisClusterClient.SCard(RequestUuidSet).Result()
	} else {
		count, err = b.redisClient.SCard(RequestUuidSet).Result()
	}
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return count, nil
}

//获取失败的任务数
func (b *Broker) GetFailTaskCount(date string) (int64, error) {
	if len(date) == 0 {
		return 0, ErrInvalidArgument
	}
	failTaskKey := fmt.Sprintf(FailTaskKey, date)
	var str string
	var err error
	if b.IsCluster() {
		str, err = b.redisClusterClient.Get(failTaskKey).Result()
	} else {
		str, err = b.redisClient.Get(failTaskKey).Result()
	}

	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, nil
	}
	count, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return 0, err
	}
	return count, nil
}

//获取成功的任务数
func (b *Broker) GetSuccessTaskCount(date string) (int64, error) {
	if len(date) == 0 {
		return 0, ErrInvalidArgument
	}
	successTaskKey := fmt.Sprintf(SuccessTaskKey, date)
	var str string
	var err error
	if b.IsCluster() {
		str, err = b.redisClusterClient.Get(successTaskKey).Result()
	} else {
		str, err = b.redisClient.Get(successTaskKey).Result()
	}
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	count, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return 0, err
	}
	return count, nil
}
