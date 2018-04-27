package main

import (
	"flag"
	"github.com/tiglabs/baudengine/util/log"
	"github.com/tiglabs/baudengine/router"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
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

func interceptSignal(s *router.Router) {
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
	fmt.Println("Hello, Baud! I router!!!")
	flag.Parse()

	cfg := router.LoadConfig(*configFile)

	//for multi-cpu scheduling
	runtime.GOMAXPROCS(runtime.NumCPU())

	//init profile server
	go func() {
		fmt.Println(http.ListenAndServe(fmt.Sprintf(":%d", cfg.Pprof), nil))
	}()

	server := router.NewServer()

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
