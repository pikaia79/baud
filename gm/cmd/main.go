package main

import (
	"flag"
	"fmt"
	"github.com/tiglabs/baudengine/gm"
	"github.com/tiglabs/baudengine/util/log"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
)

var (
	configFile = flag.String("c", "", "config file path")
	mainWg     sync.WaitGroup
)

type IServer interface {
	Start(cfg *gm.Config) error
	Shutdown()
}

func interceptSignal(s IServer) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		signal := <-sigs
		log.Info("master received signal[%v]", signal.String())

		s.Shutdown()
		mainWg.Done()
		os.Exit(0)
	}()
}

func main() {
	fmt.Println("Hello, BaudEngine GM!")
	flag.Parse()
	fmt.Printf("configfile=[%v]\n", *configFile)

	//for multi-cpu scheduling
	runtime.GOMAXPROCS(runtime.NumCPU())

	cfg := gm.NewConfig(*configFile)

	log.InitFileLog(cfg.LogCfg.LogPath, cfg.ModuleCfg.Name, cfg.LogCfg.Level)
	log.Debug("log has been initialized")

	server := gm.NewServer()

	mainWg.Add(1)
	//install the signal handler
	interceptSignal(server)

	//start the server
	err := server.Start(cfg)
	if err != nil {
		log.Fatal("Fatal: failed to start the Baud Master daemon - ", err)
	}

	log.Info("Baud GM is running!")
	mainWg.Wait()
	log.Info("Goodbye, Baud GM!")
}
