package main

import (
	"flag"
	"github.com/tiglabs/baudengine/util/log"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"sync"
	"fmt"
	"github.com/tiglabs/baudengine/zm"
)

const (
	Version     = "0.1"
	LogicalCPUs = 32
)

var (
	configFile = flag.String("c", "", "config file path")

	mainWg sync.WaitGroup
)

func interceptSignal(s *zm.ZoneMaster) {
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
	fmt.Println("Hello, BaudEsngine Zone Master!")
	flag.Parse()
	fmt.Printf("configfile=[%v]\n", *configFile)

	//for multi-cpu scheduling
	runtime.GOMAXPROCS(runtime.NumCPU())

	cfg := zm.NewConfig(*configFile)

	log.InitFileLog(cfg.LogCfg.LogPath, cfg.ModuleCfg.Name, cfg.LogCfg.Level)
	log.Debug("log has been initialized")

	server := zm.NewServer()

	mainWg.Add(1)
	//install the signal handler
	interceptSignal(server)

	//start the server
	err := server.Start(cfg)
	if err != nil {
		log.Fatal("Fatal: failed to start the Baud Zone Master daemon - ", err)
	}

	log.Info("Baud Master is running!")
	mainWg.Wait()
	log.Info("Goodbye, Baud Zone Master!")
}
