package topo

import (
    "context"
    "github.com/tiglabs/baudengine/proto/metapb"
    "github.com/golang/protobuf/proto"
    "path"
    "github.com/tiglabs/baudengine/util/log"
    "fmt"
)

type DBTopo struct {
    version Version
    *metapb.DB
}

type DBWatchData struct {
    *DBTopo
    Err error
}

//func (s *TopoServer) GetAllDBs(ctx context.Context) ([]*DBTopo, error) {
//    dbs, _, err := s.doGetAllDBs(ctx)
//    return dbs, err
//}

func (s *TopoServer) GetAllDBs(ctx context.Context) ([]*DBTopo, error) {
    if ctx == nil {
        return nil, ErrNoNode
    }

    dbIds, _, err := s.backend.ListDir(ctx, GlobalZone, dbsPath)
    if err != nil {
        return nil, err
    }
    if dbIds == nil || len(dbIds) == 0 {
        return nil, nil
    }

    dbs := make([]*DBTopo, 0, len(dbIds))
    for _, dbId := range dbIds {
        contents, version, err := s.backend.Get(ctx, GlobalZone, path.Join(dbsPath, fmt.Sprint(dbId), DBTopoFile))
        if err != nil {
            return nil, err
        }

        dbMeta := &metapb.DB{}
        if err := proto.Unmarshal(contents, dbMeta); err != nil {
            log.Error("Fail to unmarshal meta data for db[%d]. err[%v]", dbId, err)
            return nil, err
        }

        db := &DBTopo{version: version, DB: dbMeta}
        dbs = append(dbs, db)
    }

    return dbs, nil
}

// []*DBWatchData : current data returned
// error          : error returned when first watching
func (s *TopoServer) WatchDBs(ctx context.Context) (error, []*DBTopo, <-chan *DBWatchData, CancelFunc) {
    if ctx == nil {
       return ErrNoNode, nil, nil, nil
    }

    dirPath := path.Join(dbsPath) + "/"
    dbIds, dirVersion, err := s.backend.ListDir(ctx, GlobalZone, dirPath)
    if err != nil && err != ErrNoNode {
       return err, nil, nil, nil
    }

    var dbs []*DBTopo
    if err != ErrNoNode && len(dbIds) != 0 {
       dbs = make([]*DBTopo, 0, len(dbIds))
       for _, dbId := range dbIds {
           contents, version, err := s.backend.Get(ctx, GlobalZone, path.Join(dbsPath, fmt.Sprint(dbId), DBTopoFile))
           if err != nil {
               return err, nil, nil, nil
           }

           dbMeta := &metapb.DB{}
           if err := proto.Unmarshal(contents, dbMeta); err != nil {
               log.Error("Fail to unmarshal meta data for db[%d]. err[%v]", dbId, err)
               return err, nil, nil, nil
           }

           db := &DBTopo{version: version, DB: dbMeta}
           dbs = append(dbs, db)
       }
    }

    wdChannel, cancel, err := s.backend.WatchDir(ctx, GlobalZone, dirPath, dirVersion)
    if err != nil {
       return err, nil, nil, nil
    }

    changes := make(chan *DBWatchData, 10)

    go func() {
       defer close(changes)

       for wd := range wdChannel {
           if wd.Err != nil {
               changes <- &DBWatchData{Err: wd.Err}
               return
           }

           value := &metapb.DB{}
           if err := proto.Unmarshal(wd.Contents, value); err != nil {
               log.Error("Fail to unmarshal meta data for db from watch. err[%v]", err)
               cancel()
               for range wdChannel {
               }
               changes <- &DBWatchData{Err: err}
               return
           }

           changes <- &DBWatchData{DBTopo: &DBTopo{DB: value, version: wd.Version}}
       }
    }()

    return nil, dbs, changes, cancel
}

func (s *TopoServer) GetDB(ctx context.Context, dbId metapb.DBID) (*DBTopo, error) {
    if ctx == nil {
        return nil, ErrNoNode
    }

    contents, version, err := s.backend.Get(ctx, GlobalZone, path.Join(dbsPath, fmt.Sprint(dbId), DBTopoFile))
    if err != nil {
        return nil, err
    }

    dbMeta := &metapb.DB{}
    if err := proto.Unmarshal(contents, dbMeta); err != nil {
        log.Error("Fail to unmarshal meta data for db[%d]. err[%v]", dbId, err)
        return nil, err
    }

    db := &DBTopo{version: version, DB: dbMeta}

    return db, nil
}

func (s *TopoServer) AddDB(ctx context.Context, db *metapb.DB) (*DBTopo, error) {
    if ctx == nil || db == nil {
        return nil, ErrNoNode
    }

    contents, err := proto.Marshal(db)
    if err != nil {
        log.Error("Fail to marshal meta data for db[%v]. err[%v]", db, err)
        return nil, err
    }

    version, err := s.backend.Create(ctx, GlobalZone, path.Join(dbsPath, fmt.Sprint(db.ID), DBTopoFile), contents)
    if err != nil {
        return nil, err
    }

    return &DBTopo{version: version, DB: db}, nil
}

func (s *TopoServer) UpdateDB(ctx context.Context, db *DBTopo) error {
    if ctx == nil || db == nil {
        return ErrNoNode
    }

    contents, err := proto.Marshal(db.DB)
    if err != nil {
        log.Error("Fail to marshal meta data for db[%v]. err[%v]", db.DB, err)
        return err
    }

    newVersion, err := s.backend.Update(ctx, GlobalZone, path.Join(dbsPath, fmt.Sprint(db.ID), DBTopoFile),
            contents, db.version)
    if err != nil {
        return err
    }
    db.version = newVersion

    return nil
}

func (s *TopoServer) DeleteDB(ctx context.Context, db *DBTopo) error {
    if ctx == nil || db == nil {
        return ErrNoNode
    }

    return s.backend.Delete(ctx, GlobalZone, path.Join(dbsPath, fmt.Sprint(db.ID), DBTopoFile), db.version)
}

//func (s *TopoServer) WatchDB(ctx context.Context, dbId metapb.DBID) (*DBWatchData, <-chan *DBWatchData, CancelFunc) {
//    if ctx == nil {
//        return &DBWatchData{Err:ErrNoNode}, nil, nil
//    }
//
//    current, wdChannel, cancel := s.backend.Watch(ctx, GlobalZone, path.Join(dbsPath, fmt.Sprint(dbId), DBTopoFile))
//    if current.Err != nil {
//        return &DBWatchData{Err:current.Err}, nil, nil
//    }
//
//    curValue := &metapb.DB{}
//    if err := proto.Unmarshal(current.Contents, curValue); err != nil {
//        log.Error("Fail to unmarshal meta data for db[%d]. err[%v]", dbId, err)
//        cancel()
//        for range wdChannel {
//        }
//        return &DBWatchData{Err: err}, nil, nil
//    }
//
//    changes := make(chan *DBWatchData, 10)
//
//    go func() {
//        defer close(changes)
//
//        for wd := range wdChannel {
//            if wd.Err != nil {
//                changes <- &DBWatchData{Err: wd.Err}
//                return
//            }
//
//            value := &metapb.DB{}
//            if err := proto.Unmarshal(wd.Contents, value); err != nil {
//                log.Error("Fail to unmarshal meta data for db from watch. err[%v]", err)
//                cancel()
//                for range wdChannel {
//                }
//                changes <- &DBWatchData{Err: err}
//                return
//            }
//
//            changes <- &DBWatchData{DBTopo: &DBTopo{DB: value, version:wd.Version}}
//        }
//    }()
//
//    return &DBWatchData{DBTopo: &DBTopo{DB: curValue, version:current.Version}}, changes, cancel
//}