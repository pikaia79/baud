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
	"sync"
	"github.com/tiglabs/baudengine/proto/metapb"
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
    defer cancel()

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
    dbMeta1 := &metapb.DB{ID:1, Name:"mydb1"}
    dbTopo1, err := server.AddDB(ctx, dbMeta1)
    if err != nil {
      log.Error("AddDB err[%v]", err)
      return
    }
    log.Debug("add new db[%v]", dbTopo1)
	dbMeta2 := &metapb.DB{ID:2, Name:"mydb2"}
	dbTopo2, err := server.AddDB(ctx, dbMeta2)
	if err != nil {
		log.Error("AddDB err[%v]", err)
		return
	}
	log.Debug("add new db[%v]", dbTopo2)
    //dbTopo, err := server.GetDB(ctx, 1)
    //if err != nil {
    //    log.Error("GetDB err[%v]", err)
    //    return
    //}
    //log.Debug("get db[%v]", dbTopo)
    //
    //dbTopos, err := server.GetAllDBs(ctx)
    //if err != nil {
    //   log.Error("GetAllDBs err[%v]", err)
    //   return
    //}
    //log.Debug("all dbTopos=%v", dbTopos)
    //dbCur, dbChannel, _ := server.WatchDB(ctx, 1)
    //if dbCur == nil {
    //   log.Error("WatchDB err[%v]", dbCur.Err)
    //   return
    //}
    //log.Debug("watched current db[%v]", dbCur)
    //var wg sync.WaitGroup
    //wg.Add(1)
    //go func() {
    //   defer wg.Done()
    //   for db := range dbChannel {
    //       if db.Err != nil {
    //           log.Error("watch err[%v]", db.Err)
    //           return
    //       }
    //       log.Debug("watched db[%v]", db.DB)
    //   }
    //}()

	err, currentDbTopos, dbChannel, _ := server.WatchDBs(ctx)
	if err != nil {
		log.Debug("WatchDBs err[%v]", err)
		return
	}
	log.Debug("current dbs[%v] before WatchDBs", currentDbTopos)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
	  defer wg.Done()
	  for db := range dbChannel {
	      if db.Err != nil {
	          log.Error("watch err[%v]", db.Err)
	          return
	      }
	      log.Debug("watched db[%v]", db.DB)
	  }
	}()

    //time.Sleep(1 * time.Second)
    //dbTopo.Name = "mydb222"
    //if err := server.UpdateDB(ctx, dbTopo); err != nil {
    //    log.Error("UpdateDB err[%v]", err)
    //    return
    //}
    wg.Wait()
    //if err := server.DeleteDB(ctx, dbTopo1); err != nil {
    //   log.Error("DeleteDB err[%v]", err)
    //   return
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
    //   log.Error("AddPsByZone err[%v]", psTopo)
    //   return
    //}
    //log.Debug("psTopo=[%v]", psTopo)
    //
    //psTopo2, err := server.GetPsByZone(ctx, "zone1", 201)
    //if err != nil {
    //	log.Error("GetPsByZone err[%v]")
    //	return
    //}
    //log.Debug("psTopo2=%v", psTopo2)
    //
    //psTopo2.AdminAddr = "127.0.0.23"
    //if err := server.UpdatePsByZone(ctx, "zone1", psTopo2); err != nil {
    //    log.Error("UpdatePsByZone err[%v]", err)
    //    return
    //}
    //log.Debug("changed psTopo2=%v", psTopo2)
    //
    //psTopos, err := server.GetAllPsByZone(ctx, "zone1")
    //if err != nil {
    //	log.Error("GetAllPsByZone err[%v]", err)
    //	return
    //}
    //log.Debug("psTopos=%v", psTopos)
    //
    //if err := server.DeletePsByZone(ctx, "zone1", psTopo2); err != nil {
    //    log.Error("DeletePsByZone err[%v]", err)
    //    return
    //}

    //mp1, err := server.NewMasterParticipation(topo.GlobalZone, "191")
    //if err != nil {
    //    log.Error("new master participation. err[%v]", err)
    //    return
    //}
    //mp2, err := server.NewMasterParticipation(topo.GlobalZone, "192")
    //if err != nil {
    //    log.Error("new master participation. err[%v]", err)
    //    return
    //}
    //var wg sync.WaitGroup
    //wg.Add(1)
    //go func() {
    //    defer wg.Done()
    //    defer func() {
    //        if e := recover(); e != nil {
    //            log.Error("recover [%v]\n[%s]", e, debug.Stack())
    //        }
    //
    //    }()
    //    _, err := mp1.WaitForMastership()
    //    if err != nil {
    //        log.Error("%v", err)
    //    }
    //    log.Info("mp1 get master")
    //
    //}()
    //wg.Add(1)
    //go func() {
    //    defer wg.Done()
    //    defer func() {
    //        if e := recover(); e != nil {
    //            log.Error("recover [%v]\n[%s]", e, debug.Stack())
    //        }
    //
    //    }()
    //    _, err := mp2.WaitForMastership()
    //    if err != nil {
    //        log.Error("%v", err)
    //    }
    //    log.Info("mp2 get master")
    //
    //}()
    //time.Sleep(time.Second)
    //masterId1, err := mp1.GetCurrentMasterID(ctx)
    //if err != nil {
    //    return
    //}
    //log.Debug("mp1 master id =%s", masterId1)
    //masterId2, err := mp2.GetCurrentMasterID(ctx)
    //if err != nil {
    //    return
    //}
    //log.Debug("mp2 master id =%s", masterId2)
    //wg.Wait()

    //mp3, err := server.NewMasterParticipation("zone1", "193")
    //if err != nil {
    //   log.Error("new master participation. err[%v]", err)
    //   return
    //}
    //var wg sync.WaitGroup
    //wg.Add(1)
    //go func() {
    //   defer wg.Done()
    //   defer func() {
    //       if e := recover(); e != nil {
    //           log.Error("recover [%v]\n[%s]", e, debug.Stack())
    //       }
    //
    //   }()
    //   _, err := mp3.WaitForMastership()
    //   if err != nil {
    //       log.Error("%v", err)
    //   }
    //   log.Info("mp3 get master")
    //
    //}()
    //time.Sleep(time.Second)
    //masterId3, err := mp3.GetCurrentMasterID(ctx)
    //if err != nil {
    //  return
    //}
    //log.Debug("mp3 master id =%s", masterId3)
    //wg.Wait()

    //var wg sync.WaitGroup
    //for i := 0; i < 100; i++ {
    //    wg.Add(1)
    //    go func() {
    //        defer wg.Done()
    //        for i := 0; i < 10; i++ {
    //            start, end, err := server.GenerateNewId(ctx, 10)
    //            if err != nil {
    //                log.Error("GenerateNewId err[%v]", err)
    //                return
    //            }
    //            log.Debug("start=%d, end=%d", start, end)
    //        }
    //    }()
    //}
    //wg.Wait()

    //err := server.SetZonesForPartition(ctx, 120, []string{"zone1", "zone2", "zone3"})
    //if err != nil {
    	//log.Error("SetZonesForPartition err[%v]", err)
    	//return
	//}
	//zones, err := server.GetZonesForPartition(ctx, 120)
	//if err != nil {
	//	log.Error("GetZonesForPartition err[%v]", err)
	//	return
	//}
	//log.Debug("zones=%v", zones)
	//err = server.SetZonesForPartition(ctx, 120, []string{"zone2", "zone4"})
	//if err != nil {
	//	log.Error("SetZonesForPartition err[%v]", err)
	//	return
	//}
	//zones, err = server.GetZonesForPartition(ctx, 120)
	//if err != nil {
	//	log.Error("GetZonesForPartition err[%v]", err)
	//	return
	//}
	//log.Debug("updateddated zones=%v", zones)


	time.Sleep(10 * time.Second)
	log.Info("Goodbye, BaudEngine Topo control!")
}
//
//func topoRecover() {
//    if e := recover(); e != nil {
//        log.Error("catch panic. error[%s]\n stack[%s]", e, debug.Stack())
//    }
//}
