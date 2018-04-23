package server

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/tiglabs/baud/proto/metapb"
	"github.com/tiglabs/baud/proto/pspb"
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
	m.mkMetaFile()

	if b, err := info.Marshal(); err == nil {
		ioutil.WriteFile(m.metaFile, b, 0666)
	}
}

func (m *serverMeta) getInfo() *pspb.MetaInfo {
	info := &pspb.MetaInfo{}
	if b, err := ioutil.ReadFile(m.metaFile); err != nil && len(b) > 0 {
		info.Unmarshal(b)
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
		}
	}
}

func getPartitionPath(id metapb.PartitionID, path string, create bool) (dir string, err error) {
	dir = filepath.Join(path, fmt.Sprintf("%d", id))
	if create {
		err = os.MkdirAll(dir, os.ModePerm)
	}

	return
}

func getDataPath(id metapb.PartitionID, path string, create bool) (dir string, err error) {
	dir = filepath.Join(path, fmt.Sprintf("%d", id), "data")
	if create {
		err = os.MkdirAll(dir, os.ModePerm)
	}

	return
}

func getRaftPath(id metapb.PartitionID, path string, create bool) (dir string, err error) {
	dir = filepath.Join(path, fmt.Sprintf("%d", id), "raft")
	if create {
		err = os.MkdirAll(dir, os.ModePerm)
	}

	return
}

type PartitionByIdSlice []metapb.Partition

func (r PartitionByIdSlice) Len() int {
	return len(r)
}

func (r PartitionByIdSlice) Swap(i int, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r PartitionByIdSlice) Less(i int, j int) bool {
	return r[i].ID < r[j].ID
}
