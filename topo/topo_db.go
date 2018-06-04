package topo

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/util/log"
	"path"
	"strconv"
	"strings"
)

type DBTopo struct {
	Version Version
	*metapb.DB
}

type DBWatchData struct {
	*DBTopo
	Err error
}

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

		db := &DBTopo{Version: version, DB: dbMeta}
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

	dbMeta := &metapb.DB{}
	if err := proto.Unmarshal(contents, dbMeta); err != nil {
		log.Error("Fail to unmarshal meta data for db[%d]. err[%v]", dbId, err)
		return nil, err
	}

	db := &DBTopo{Version: version, DB: dbMeta}

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

	return &DBTopo{Version: version, DB: db}, nil
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
		contents, db.Version)
	if err != nil {
		return err
	}
	db.Version = newVersion

	return nil
}

func (s *TopoServer) DeleteDB(ctx context.Context, db *DBTopo) error {
	if ctx == nil || db == nil {
		return ErrNoNode
	}

	return s.backend.Delete(ctx, GlobalZone, path.Join(dbsPath, fmt.Sprint(db.ID), DBTopoFile), db.Version)
}

// []*DBTopo : initial children dbs returned
// error     : error returned when first watching
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

			db := &DBTopo{Version: version, DB: dbMeta}
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
			if wd.Err != nil && wd.Err != ErrNoNode {
				changes <- &DBWatchData{Err: wd.Err}
				return
			}

			if wd.Err == ErrNoNode { // node deleted
				keyDel := string(wd.Contents)
				segs := strings.Split(keyDel, "/")
				if len(segs) < 3 {
					changes <- &DBWatchData{Err: ErrInvalidPath}
					return
				}
				dbId, err := strconv.Atoi(segs[1])
				if err != nil {
					changes <- &DBWatchData{Err: ErrInvalidPath}
					return
				}

				value := &metapb.DB{ID: metapb.DBID(dbId)}
				changes <- &DBWatchData{Err: ErrNoNode, DBTopo: &DBTopo{DB: value, Version: wd.Version}}

			} else { // node added or updated
				value := &metapb.DB{}
				if err := proto.Unmarshal(wd.Contents, value); err != nil {
					log.Error("Fail to unmarshal meta data for db from watch. err[%v]", err)
					cancel()
					for range wdChannel {
					}
					changes <- &DBWatchData{Err: err}
					return
				}

				changes <- &DBWatchData{DBTopo: &DBTopo{DB: value, Version: wd.Version}}
			}
		}
	}()

	return nil, dbs, changes, cancel
}
