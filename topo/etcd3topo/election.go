// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package etcd3topo

import (
	"path"

	"github.com/coreos/etcd/clientv3"
	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/tiglabs/baudengine/topo"
)

// NewMasterParticipation is part of the topo.Server interface
func (s *Server) NewMasterParticipation(cell, id string) (topo.MasterParticipation, error) {
	return &etcdMasterParticipation{
		s:       s,
		cell:    cell,
		id:      id,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}, nil
}

// etcdMasterParticipation implements topo.MasterParticipation.
//
// We use a directory (in global election path, with the name) with
// ephemeral files in it, that contains the id.  The oldest revision
// wins the election.
type etcdMasterParticipation struct {
	// s is our parent etcd topo Server
	s *Server

	// name is the name of this MasterParticipation
	cell string

	// id is the process's current id.
	id string

	// stop is a channel closed when Stop is called.
	stop chan struct{}

	// done is a channel closed when we're done processing the Stop
	done chan struct{}
}

// WaitForMastership is part of the topo.MasterParticipation interface.
func (mp *etcdMasterParticipation) WaitForMastership() (context.Context, error) {
	electionPath := mp.buildElectionPath()
	lockPath := ""

	// We use a cancelable context here. If stop is closed,
	// we just cancel that context.
	lockCtx, lockCancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-mp.stop:
			if lockPath != "" {
				if err := mp.s.unlock(context.Background(), mp.cell, electionPath, lockPath); err != nil {
					log.Errorf("failed to delete lockPath %v for election %v: %v", lockPath, mp.cell, err)
				}
			}
			lockCancel()
			close(mp.done)
		}
	}()

	// Try to get the mastership, by getting a lock.
	var err error
	lockPath, err = mp.s.lock(lockCtx, mp.cell, electionPath, mp.id)
	if err != nil {
		// It can be that we were interrupted.
		return nil, err
	}

	// We got the lock. Return the lockContext. If Stop() is called,
	// it will cancel the lockCtx, and cancel the returned context.
	return lockCtx, nil
}

// Stop is part of the topo.MasterParticipation interface
func (mp *etcdMasterParticipation) Stop() {
	close(mp.stop)
	<-mp.done
}

// GetCurrentMasterID is part of the topo.MasterParticipation interface
func (mp *etcdMasterParticipation) GetCurrentMasterID(ctx context.Context) (string, error) {
	c, err := mp.s.clientForCell(ctx, mp.cell)
	if err != nil {
		return "", err
	}

	electionPath := path.Join(c.root, electionsPath, mp.cell)

	// Get the keys in the directory, older first.
	resp, err := c.cli.Get(ctx, electionPath+"/",
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortAscend),
		clientv3.WithLimit(1))
	if err != nil {
		return "", convertError(err)
	}
	if len(resp.Kvs) == 0 {
		// No key starts with this prefix, means nobody is the master.
		return "", nil
	}
	return string(resp.Kvs[0].Value), nil
}

func (mp *etcdMasterParticipation) buildElectionPath() string {
	var electionPath string
	if mp.cell == topo.GlobalZone {
		electionPath = path.Join(mp.s.global.root, electionsPath, mp.cell)
	} else {
		client, ok := mp.s.cells[mp.cell]
		if ok {
			electionPath = path.Join(client.root, electionsPath, mp.cell)
		} else {
			log.Error("cell[%s] not found", mp.cell)
			electionPath = ""
		}
	}

	return electionPath
}
