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
    "runtime/debug"
)

var (
	logPath  = flag.String("log_path", "/tmp/baudengine/topoctl/log", "log file path")
	logName  = flag.String("log_name", "topoctl", "log module name")
	logLevel = flag.String("log_level", "debug", "log level [debug, info, warn, error]")
)

func main() {
	fmt.Println("Hello, BaudEngine Topo control!")

    defer func() {
        if e := recover(); e != nil {
            log.Error("recover worker panic. e[%s] \nstack:[%s]", e, debug.Stack())
            time.Sleep(5 * time.Second)
        }
    }()

	flag.Parse()

	log.InitFileLog(*logPath, *logName, *logLevel)

	//for multi-cpu scheduling
	runtime.GOMAXPROCS(runtime.NumCPU())

	server := topo.Open()
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30 * time.Second)

	//zoneMeta := &metapb.Zone{Name:"zone1", ServerAddrs:"127.0.0.1:9302", RootDir:"/zones/zone1"}
	//_, err := server.AddZone(ctx, zoneMeta)
	//if err != nil {
	//	log.Error("addzone1 zone1 err[%v]", err)
	//	return
	//}
    //
    //zoneTopos, err := server.GetAllZones(ctx)
    //if err != nil {
		//log.Error("GetAllZones err[%v]", err)
		//return
    //}
    //log.Debug("zoneTopos=%v", zoneTopos)
    //
    //if err := server.DeleteZone(ctx, zone1); err != nil {
	 //   log.Error("DeleteZone err[%v]", err)
	 //   return
    //}
    //
    //dbMeta := &metapb.DB{ID:1, Name:"mydb1"}
    //dbTopo1, err := server.AddDB(ctx, dbMeta)
    //if err != nil {
    //    log.Error("AddDB err[%v]", err)
    //    return
    //}
    //
    //dbTopos, err := server.GetAllDBs(ctx)
    //if err != nil {
    //    log.Error("GetAllDBs err[%v]", err)
    //    return
    //}
    //log.Debug("all dbTopos=%v", dbTopos)
    //
    //if err := server.DeleteDB(ctx, dbTopo1); err != nil {
    //    log.Error("DeleteDB err[%v]", err)
    //    return
    //}

   // spaceMeta1 := &metapb.Space{ID:11, DB:1, Name:"myspace1"}
   // partitionMetas := make([]*metapb.Partition, 0, 2)
   // partitionMetas = append(partitionMetas, &metapb.Partition{ID: 101, DB:1, Space:11, StartSlot:0, EndSlot:200})
   // partitionMetas = append(partitionMetas, &metapb.Partition{ID: 102, DB:1, Space:11, StartSlot:200, EndSlot:400})
    //spaceTopo1, partitionTopos, err := server.AddSpace(ctx, spaceMeta1, partitionMetas)
    //if err != nil {
    //    log.Error("AddSpace err[%v]", err)
    //    return
    //}
    //log.Debug("spaceTopo1=%v, partitionTopos=%v", spaceTopo1, partitionTopos)
    //
    //space, err := server.GetSpace(ctx, 1, 11)
    //if err != nil {
    //   log.Error("GetSpace err[%v]", err)
    //   return
    //}
    //log.Debug("space=%v", space)
    //
    //space.Name = "myspace2"
    //if err := server.UpdateSpace(ctx, space); err != nil {
    //    log.Error("UpdateSpace err[%v]", err)
    //    return
    //}
    //
    //spaceTopos, err := server.GetAllSpaces(ctx)
    //if err != nil {
    //   log.Error("GetAllSpace err[%v]", err)
    //   return
    //}
    //log.Debug("allspace=%v", spaceTopos)
    //
    //if err := server.DeleteSpace(ctx, space); err != nil {
    //    log.Error("DeleteSpace err[%v]", err)
    //    return
    //}
    //
    //psMeta := &metapb.Node{ID:201, Ip:"127.0.0.9"}
    //psTopo, err := server.AddPsByZone(ctx, "zone1", psMeta)
    //if err != nil {
    //    log.Error("AddPsByZone err[%v]", psTopo)
    //    return
    //}
    //log.Debug("psTopo=[%v]", psTopo)

    psTopo2, err := server.GetPsByZone(ctx, "zone1", 201)
    if err != nil {
    	log.Error("GetPsByZone err[%v]")
    	return
	}
	log.Debug("psTopo2=%v", psTopo2)

    psTopos, err := server.GetAllPsByZone(ctx, "zone1")
    if err != nil {
    	log.Error("GetAllPsByZone err[%v]", err)
    	return
	}
	log.Debug("psTopos=%v", psTopos)


	cancel()
	time.Sleep(5 * time.Second)
	log.Info("Goodbye, BaudEngine Topo control!")
}
//
//func topoRecover() {
//    if e := recover(); e != nil {
//        log.Error("catch panic. error[%s]\n stack[%s]", e, debug.Stack())
//    }
//}
