## 异步任务调度工具

基于echo.v2和kingtask,可通过redis进行任务的注册与查询

架构图:

```

            +------------------+
            |                  |
            |     broker       |
            |                  |
            +---------+--------+
                      |
                      |
            +---------v--------+
            |                  |
     +-----^+     redis        +^-------+
     |      |                  |        |
     |      +----------+-------+        |
     |                 ^                |
     |                 |                |
     |                 |                |
+----+-------+ +-------+----+  +--------+---+
|            | |            |  |            |
|   worker   | |   worker   |  |   worker   |
|            | |            |  |            |
+------------+ +------------+  +------------+

```

配置broker (参考 config/broker.yaml)
```go
#broker端口配置
port : :9595
#redis地址
redis : 192.168.139.139:6699
#log输出到文件，可不配置
#log_path: /Users/lihaoquan/Desktop/taskbin/logs
#日志级别
log_level: debug
```

配置worker
```go
#redis地址
redis : 192.168.139.139:6699
#异步任务可执行文件目录
bin_path : /Users/lihaoquan/Desktop/taskbin
#日志输出目录，可不配置
#log_path : /Users/lihaoquan/Desktop/taskbin/logs
#日志级别
log_level: debug

#每个任务执行时间间隔，单位为秒
period : 1
#结果保存时间，单位为秒
result_keep_time : 1000
#任务执行最长时间，单位秒
task_run_time: 30
```

运行broker
```go
go run broker/main.go  -config=config/broker.yaml
```

运行worker
```go
go run worker/main.go  -config=config/worker.yaml
```


(1). 执行脚本异步任务API接口
```go
POST /api/task/script
```

请求参数
-bin_name 字符串类型，表示异步对应的可执行文件名，必须提供
-args 字符串类型，执行参数，多个参数用空格分隔，可为空
-start_time 整型，异步任务开始执行时刻，为空表示立刻执行，可为空
-time_interval 字符串类型，表示失败后重试的时间间隔序列，可为空
-max_run_time 整型，异步任务最长运行时间（单位为秒),超过将会被系统kill，为空则使用系统统一的超时时长


(2). 执行RPC异步任务API接口
```go
POST /api/task/rpc
```

请求参数
-method 请求类型：GET,PUT,POST,DELETE
-url 异步任务对应的URL,需要加单引号
-args json Marshal后的字符串,需要加单引号
-start_time 整型，异步任务开始执行时刻，为空表示立刻执行，可为空
-time_interval 字符串类型，表示失败后重试的时间间隔序列，可为空
-max_run_time  整型，异步任务最长运行时间（单位为秒),超过将会被系统kill，为空则使用系统统一的超时时长


(3). 查看异步任务结果API接口
```go
GET /api/task/result/:uuid
```
(4). 统计查看积压任务个数
```go
http GET 127.0.0.1:9595/api/task/count/undo
```
