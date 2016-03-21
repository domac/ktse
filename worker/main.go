package main

import (
	"flag"
	"fmt"
	"github.com/phillihq/ktse/core"
	"github.com/phillihq/ktse/logger"
	"os"
	"os/signal"
	"runtime"
	"syscall"
)

var configFile *string = flag.String("config", "./config/worker.yaml", "worker config file")

//是否采用集群模式
var clusterFlag *bool = flag.Bool("c", false, "connect to redis cluster")

const (
	sysLogName = "sys.log"
	MaxLogSize = 1024 * 1024 * 1024
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	fmt.Println("clusterFlag:", *clusterFlag)

	if len(*configFile) == 0 {
		fmt.Println("must use a config file")
		return
	}

	cfg, err := core.ParseWorkerConfigFile(*configFile)
	if err != nil {
		fmt.Printf("parse config file error:%v\n", err.Error())
		return
	}

	var w *core.Worker
	w, err = core.NewWorker(cfg, *clusterFlag)
	if err != nil {
		logger.GetLogger().Errorln(err.Error())
		w.Close()
		return
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)

	go func() {
		sig := <-sc
		logger.GetLogger().Errorln("Got signal", sig)
		w.Close()
	}()

	logger.GetLogger().Infoln("Worker start!")
	//启动worker
	w.Run()
}
