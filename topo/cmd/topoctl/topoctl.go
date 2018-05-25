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
	"github.com/tiglabs/baudengine/proto/metapb"
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

	ctx, cancel := context.WithTimeout(context.Background(), 30 * time.Second)

	zoneMeta := &metapb.Zone{Name:"zone1", ServerAddrs:"127.0.0.1:9302", RootDir:"/zones/zone1"}
	zone1, err := server.AddZone(ctx, zoneMeta)
	if err != nil {
		log.Error("addzone1 zone1 err[%v]", err)
		return
	}


	zoneMetas, err := server.GetAllZones(ctx)
	if err != nil {
		log.Error("GetAllZones err[%v]", err)
		return
	}
	log.Debug("zoneMetas=%v", zoneMetas)

	if err := server.DeleteZone(ctx, zone1); err != nil {
	    log.Error("")
	    return
    }

	cancel()

	log.Info("Goodbye, BaudEngine Topo control!")
}
