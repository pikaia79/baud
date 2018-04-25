package main

import (
	"flag"
	"github.com/tiglabs/baud/util/log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"github.com/tiglabs/baud/master"
	"github.com/tiglabs/raft/logger"
	"sync"
	"fmt"
)

const (
	Version     = "0.1"
	LogicalCPUs = 32
)

var (
	configFile = flag.String("c", "", "config file path")
	logLevel   = flag.Int("log", 0, "log level, as DebugLevel = 0")

	mainWg sync.WaitGroup
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
	fmt.Println("Hello, Baud!")
	flag.Parse()
	fmt.Printf("configFile=[%v]\n", *configFile)

	cfg := master.NewConfig(*configFile)

	log.InitFileLog(cfg.LogCfg.LogPath, cfg.ModuleCfg.Name, cfg.LogCfg.Level)
	logger.SetLogger(log.GetFileLogger().SetRaftLevel(cfg.LogCfg.RaftLevel))
	log.Debug("log initialized")

	role := "master"
	profPort := "50000"

	//for multi-cpu scheduling
	runtime.GOMAXPROCS(runtime.NumCPU())

	//init profile server
	go func() {
		fmt.Println(http.ListenAndServe(":"+profPort, nil))
	}()

	var server IServer

	switch role {
	case "master":
		server = master.NewServer()
	case "ps":
		//server = partition.NewServer()
	case "router":
		//server = router.NewServer()

	default:
		fmt.Println("Fatal: unmath role: ", role)
		return
	}

	mainWg.Add(1)
	//install the signal handler
	interceptSignal(server)

	//start the server
	err := server.Start(cfg)
	if err != nil {
		log.Fatal("Fatal: failed to start the Baud daemon - ", err)
	}

	log.Info("master main is running")
	mainWg.Wait()
	log.Info("master main exit")
}
