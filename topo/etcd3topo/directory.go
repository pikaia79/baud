package etcd3topo

import (
	"path"
	"strings"

	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/tiglabs/baudengine/topo"
	"golang.org/x/net/context"
)

// ListDir is part of the topo.Backend interface.
func (s *Server) ListDir(ctx context.Context, cell, dirPath string) ([]string, topo.Version, error) {
	c, err := s.clientForCell(ctx, cell)
	if err != nil {
		return nil, nil, err
	}
	nodePath := path.Join(c.root, dirPath) + "/"
	resp, err := c.cli.Get(ctx, nodePath,
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithKeysOnly())
	if err != nil {
		return nil, nil, convertError(err)
	}
	if len(resp.Kvs) == 0 {
		// No key starts with this prefix, means the directory
		// doesn't exist.
		return nil, nil, topo.ErrNoNode
	}

	prefixLen := len(nodePath)
	var result []string
	for _, ev := range resp.Kvs {
		p := string(ev.Key)

		// Remove the prefix, base path.
		if !strings.HasPrefix(p, nodePath) {
			return nil, nil, ErrBadResponse
		}
		p = p[prefixLen:]

		// Keep only the part until the first '/'.
		if i := strings.Index(p, "/"); i >= 0 {
			p = p[:i]
		}

		// Remove duplicates, add to list.
		if len(result) == 0 || result[len(result)-1] != p {
			result = append(result, p)
		}
	}

	return result, EtcdVersion(resp.Header.Revision), nil
}

func (s *Server) WatchDir(ctx context.Context, cell, dirPath string, version topo.Version) (<-chan *topo.WatchData, topo.CancelFunc, error) {
	c, err := s.clientForCell(ctx, cell)
	if err != nil {
		return nil, nil, fmt.Errorf("Watch cannot get cell: %v", err)
	}

	nodePath := path.Join(c.root, dirPath)
	watchCtx, watchCancel := context.WithCancel(context.Background())

	options := make([]clientv3.OpOption, 0)
	options = append(options, clientv3.WithPrefix())
	if version != nil {
		options = append(options, clientv3.WithRev(int64(version.(EtcdVersion))))
	} else {
		options = append(options, clientv3.WithRev(0))
	}
	watcher := c.cli.Watch(watchCtx, nodePath+"/", options...)
	if watcher == nil {
		return nil, nil, fmt.Errorf("Watch failed")
	}

	// Create the notifications channel, send updates to it.
	notifications := make(chan *topo.WatchData, 10)
	go func() {
		defer close(notifications)

		for {
			select {
			case <-watchCtx.Done():
				// This includes context cancelation errors.
				notifications <- &topo.WatchData{
					Err: convertError(watchCtx.Err()),
				}
				return
			case wresp := <-watcher:
				if wresp.Canceled {
					// Final notification.
					notifications <- &topo.WatchData{
						Err: convertError(wresp.Err()),
					}
					return
				}

				for _, ev := range wresp.Events {
					switch ev.Type {
					case mvccpb.PUT:
						notifications <- &topo.WatchData{
							Contents: ev.Kv.Value,
							Version:  EtcdVersion(ev.Kv.Version),
						}
					case mvccpb.DELETE:
						// Node is gone, send a final notice.
						notifications <- &topo.WatchData{
							Err: topo.ErrNoNode,
						}
						return
					default:
						notifications <- &topo.WatchData{
							Err: fmt.Errorf("unexpected event received: %v", ev),
						}
						return
					}
				}
			}
		}
	}()

	return notifications, topo.CancelFunc(watchCancel), nil
}
