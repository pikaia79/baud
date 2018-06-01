package server

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"strconv"

	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/proto/pspb"
)

type serverMeta struct {
	rootPath string
	metaFile string
}

func newServerMeta(root string) *serverMeta {
	m := &serverMeta{
		rootPath: root,
		metaFile: filepath.Join(root, "meta"),
	}
	m.mkMetaFile()

	return m
}

func (m *serverMeta) reset(info *pspb.MetaInfo) {
	if b, err := info.Marshal(); err == nil {
		if err := ioutil.WriteFile(m.metaFile, b, 0666); err != nil {
			panic(err)
		}
	}
}

func (m *serverMeta) getInfo() *pspb.MetaInfo {
	info := &pspb.MetaInfo{}
	if b, err := ioutil.ReadFile(m.metaFile); err == nil && len(b) > 0 {
		info.Unmarshal(b)
	} else if err != nil {
		panic(err)
	}

	return info
}

func (m *serverMeta) mkMetaFile() {
	if err := os.MkdirAll(m.rootPath, os.ModePerm); err != nil {
		panic(err)
	}

	if _, err := os.Stat(m.metaFile); err != nil {
		if !os.IsNotExist(err) {
			os.Remove(m.metaFile)
		}
		if file, err := os.Create(m.metaFile); err == nil {
			file.Close()
		} else {
			panic(err)
		}
	}
}

func (m *serverMeta) getAllPartitions() []metapb.PartitionID {
	ids := make(map[string]struct{}, 64)

	if dir, err := ioutil.ReadDir(filepath.Join(m.rootPath, "data")); err == nil {
		for _, fi := range dir {
			if fi.IsDir() {
				ids[fi.Name()] = struct{}{}
			}
		}
	}
	if dir, err := ioutil.ReadDir(filepath.Join(m.rootPath, "raft")); err == nil {
		for _, fi := range dir {
			if fi.IsDir() {
				ids[fi.Name()] = struct{}{}
			}
		}
	}

	retVal := make([]metapb.PartitionID, 0, len(ids))
	for id := range ids {
		if v, err := strconv.ParseUint(id, 10, 64); err == nil {
			retVal = append(retVal, v)
		}
	}

	return retVal
}

func (m *serverMeta) getDataAndRaftPath(id metapb.PartitionID) (data, raft string, err error) {
	data = filepath.Join(m.rootPath, "data", fmt.Sprintf("%d", id))
	if err = os.MkdirAll(data, os.ModePerm); err != nil {
		return
	}

	raft = filepath.Join(m.rootPath, "raft", fmt.Sprintf("%d", id))
	err = os.MkdirAll(raft, os.ModePerm)
	return
}

func (m *serverMeta) clear(id metapb.PartitionID) {
	data := filepath.Join(m.rootPath, "data", fmt.Sprintf("%d", id))
	raft := filepath.Join(m.rootPath, "raft", fmt.Sprintf("%d", id))
	os.RemoveAll(data)
	os.RemoveAll(raft)
}

func (m *serverMeta) clearAll() {
	data := filepath.Join(m.rootPath, "data")
	raft := filepath.Join(m.rootPath, "raft")
	os.RemoveAll(data)
	os.RemoveAll(raft)
}

type partitionByIDSlice []metapb.Partition

func (r partitionByIDSlice) Len() int {
	return len(r)
}

func (r partitionByIDSlice) Swap(i int, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r partitionByIDSlice) Less(i int, j int) bool {
	return r[i].ID < r[j].ID
}
