package main

import (
	"flag"
	"github.com/tiglabs/baudengine/util/log"
	_ "net/http/pprof"
	_ "github.com/tiglabs/baudengine/topo/etcd3topo"
	"github.com/tiglabs/baudengine/topo"
	"fmt"
	"runtime"
	"context"
	"time"
)

var (
	logPath  = flag.String("log_path", "/tmp/baudengine/topoctl/log", "log file path")
	logName  = flag.String("log_name", "topoctl", "log module name")
	logLevel = flag.String("log_level", "debug", "log level [debug, info, warn, error]")
)

func main() {
	fmt.Println("Hello, BaudEngine Topo control!")

	flag.Parse()

	log.InitFileLog(*logPath, *logName, *logLevel)

	//for multi-cpu scheduling
	runtime.GOMAXPROCS(runtime.NumCPU())

	server := topo.Open()
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second)
	server.GetAllZones(ctx)
	cancel()

	log.Info("Goodbye, BaudEngine Topo control!")
}
