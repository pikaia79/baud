package main

import (
	"flag"
	"github.com/tiglabs/baud/util/log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"github.com/tiglabs/baud/master"
	"github.com/tiglabs/raft/logger"
	"sync"
	"fmt"
	"runtime"
)

const (
	profPort = "50000"
)

var (
	configFile = flag.String("c", "", "config file path")
	mainWg     sync.WaitGroup
)

type IServer interface {
	Start(cfg *master.Config) error
	Shutdown()
}

func interceptSignal(s IServer) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		s.Shutdown()
		mainWg.Done()
		os.Exit(0)
	}()
}

func main() {
	fmt.Println("Hello, Baud Master!")
	flag.Parse()
	fmt.Printf("configfile=[%v]\n", *configFile)

	//for multi-cpu scheduling
	runtime.GOMAXPROCS(runtime.NumCPU())

	cfg := master.NewConfig(*configFile)

	log.InitFileLog(cfg.LogCfg.LogPath, cfg.ModuleCfg.Name, cfg.LogCfg.Level)
	logger.SetLogger(log.GetFileLogger().SetRaftLevel(cfg.LogCfg.RaftLevel))
	log.Debug("log initialized")

	//init profile server
	go func() {
		fmt.Println(http.ListenAndServe(":"+profPort, nil))
	}()

	server := master.NewServer()

	mainWg.Add(1)
	//install the signal handler
	interceptSignal(server)

	//start the server
	err := server.Start(cfg)
	if err != nil {
		log.Fatal("Fatal: failed to start the Baud Master daemon - ", err)
	}

	log.Info("Baud Master is running!")
	mainWg.Wait()
	log.Info("Goodbye, Baud Master!")
}
