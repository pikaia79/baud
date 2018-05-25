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

func (s *TopoServer) GetAllDBs(ctx context.Context) ([]*DBTopo, error) {
    if ctx == nil {
        return nil, ErrNoNode
    }

    dbIds, err := s.backend.ListDir(ctx, GlobalZone, dbsPath)
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

        db := &DBTopo{version: version}
        if err := proto.Unmarshal(contents, db.DB); err != nil {
            log.Error("Fail to unmarshal meta data for db[%d]. err[%v]", dbId, err)
            return nil, err
        }

        dbs = append(dbs, db)
    }

    return dbs, nil
}

func (s *TopoServer) GetDB(ctx context.Context, dbId metapb.DBID) (*DBTopo, error) {
    if ctx == nil {
        return nil, ErrNoNode
    }

    contents, version, err := s.backend.Get(ctx, GlobalZone, path.Join(dbsPath, fmt.Sprint(dbId), DBTopoFile))
    if err != nil {
        return nil, err
    }

    db := &DBTopo{version: version}
    if err := proto.Unmarshal(contents, db.DB); err != nil {
        log.Error("Fail to unmarshal meta data for db[%d]. err[%v]", dbId, err)
        return nil, err
    }

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