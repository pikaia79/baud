package server

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/tiglabs/baud/proto/metapb"
	"github.com/tiglabs/baud/proto/pspb"
)

type serverMeta struct {
	fileName string
	sync.Mutex
	pspb.MetaInfo
}

func newServerMeta(dataPath string) *serverMeta {
	meta := &serverMeta{
		fileName: filepath.Join(dataPath, "meta"),
	}

	if _, err := os.Stat(meta.fileName); err != nil {
		if file, err := os.Create(meta.fileName); err == nil {
			file.Close()
		} else {
			panic(err)
		}
	} else {
		if b, err := ioutil.ReadFile(meta.fileName); err != nil {
			panic(err)
		} else if len(b) > 0 {
			info := new(pspb.MetaInfo)
			if err := info.Unmarshal(b); err == nil {
				meta.MetaInfo = *info
			}
		}
	}

	return meta
}

func (m *serverMeta) getInfo() pspb.MetaInfo {
	m.Lock()
	defer m.Unlock()

	return m.MetaInfo
}

func (m *serverMeta) addPartition(id metapb.PartitionID) {
	m.Lock()
	m.MetaInfo.Partitions = append(m.MetaInfo.Partitions, id)
	if b, err := m.Marshal(); err == nil {
		ioutil.WriteFile(m.fileName, b, 0666)
	}
	m.Unlock()
}

func (m *serverMeta) reset(info *pspb.MetaInfo) {
	m.Lock()
	m.MetaInfo = *info
	if b, err := m.Marshal(); err == nil {
		ioutil.WriteFile(m.fileName, b, 0666)
	}
	m.Unlock()
}

func getPartitionPath(id metapb.PartitionID, path string) string {
	dir := filepath.Join(path, fmt.Sprintf("%d", id))
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		panic(err)
	}
	return dir
}

func getDataPath(id metapb.PartitionID, path string) string {
	dir := filepath.Join(path, fmt.Sprintf("%d", id), "data")
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		panic(err)
	}
	return dir
}

func getRaftPath(id metapb.PartitionID, path string) string {
	dir := filepath.Join(path, fmt.Sprintf("%d", id), "raft")
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		panic(err)
	}
	return dir
}
