package topo

import (
	"context"
	"github.com/tiglabs/baudengine/util"
	"path"
)

func (s *TopoServer) GenerateNewId(ctx context.Context, step uint64) (start, end uint64, err error) {
	if ctx == nil {
		return 0, 0, ErrNoNode
	}

	var retry int
	for {
		nodePath := path.Join(IdGeneratorTopoFile)
		contents, version, err := s.backend.Get(ctx, GlobalZone, nodePath)
		if err != nil {
			return 0, 0, err
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

		if retry >= 3 {
			return 0, 0, ErrBadVersion
		}
		retry++
	}
}
