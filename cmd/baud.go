package main

import (
	"flag"
	"util/log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"router"
	"sync"
	"util/config"
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
	Start(cfg *config.Config) error
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
	cfg := config.LoadConfigFile(*configFile)
	role := cfg.GetString("role")
	profPort := cfg.GetString("pprof")

	//for multi-cpu scheduling
	runtime.GOMAXPROCS(runtime.NumCPU())

	//init profile server
	go func() {
		fmt.Println(http.ListenAndServe(":"+profPort, nil))
	}()

	var server IServer

	switch role {
	case "master":
		//server = master.NewServer()
	case "ps":
		//server = partition.NewServer()
	case "router":
		server = router.NewServer()

	default:
		fmt.Println("Fatal: unmath role: ", role)
		return
	}

	//install the signal handler
	interceptSignal(server)

	//start the server
	err := server.Start(cfg)
	if err != nil {
		log.Fatal("Fatal: failed to start the Baud daemon - ", err)
	}

	log.Info("main waiting")
	mainWg.Wait()
	log.Info("main exit")
}
