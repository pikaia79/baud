package topo

import (
	"context"
	"github.com/tiglabs/baudengine/util"
	"path"
	"sync"
	"time"
	"github.com/tiglabs/baudengine/util/log"
)

var (
	idGenOnce sync.Once
)

func (s *TopoServer) GenerateNewId(ctx context.Context, step uint64) (start, end uint64, err error) {
	if ctx == nil {
		return 0, 0, ErrNoNode
	}

	var retry int
	for {
		retry++
		if retry >= 3 {
			return 0, 0, ErrBadVersion
		}

		nodePath := path.Join(IdGeneratorTopoFile)
		contents, version, err := s.backend.Get(ctx, GlobalZone, nodePath)
		if err != nil && err != ErrNoNode {
			return 0, 0, err
		}
		if err == ErrNoNode {
			ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
			if _, err := s.backend.Create(ctx, GlobalZone, nodePath, make([]byte, 8, 8)); err == nil {
				log.Info("Create initial directory idgen at one time")
			}
			continue
		}
		if len(contents) == 0 {
			return 0, 0, ErrNoNode
		}

		start = util.BytesToUint64(contents)
		end = start + step
		_, err = s.backend.Update(ctx, GlobalZone, nodePath, util.Uint64ToBytes(end), version)
		if err != nil && err != ErrBadVersion {
			return 0, 0, err
		}

		if err == nil {
			return start, end, nil
		}
	}
}
