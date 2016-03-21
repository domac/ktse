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

var configFile *string = flag.String("config", "./config/broker.yaml", "broker config file")

//是否采用集群模式
var clusterFlag *bool = flag.Bool("c", false, "connect to redis cluster")

const (
	sysLogName = "sys.log"
	MaxLogSize = 1024 * 1024 * 1024
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	if len(*configFile) == 0 {
		fmt.Println("must use a config file")
		return
	}

	cfg, err := core.ParseBrokerConfigFile(*configFile)
	if err != nil {
		fmt.Printf("parse config file error: %v\n", err.Error())
		return
	}

	var bk *core.Broker
	bk, err = core.NewBroker(cfg, *clusterFlag)
	if err != nil {
		logger.GetLogger().Errorln(err.Error())
		bk.Close()
		return
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sc
		logger.GetLogger().Errorln("Got signal", sig)
		bk.Close()
	}()

	logger.GetLogger().Infoln("Broker start!")
	bk.Run()
}
