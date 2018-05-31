package badgerdb

import (
	"os"
	"fmt"

	"github.com/dgraph-io/badger"
	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"
)

const (
	Name                    = "badgerdb"
)

var _ store.KVStore = &Store{}

type Store struct {
	path string
	db   *badger.DB
	mo store.MergeOperator
}

func New(mo store.MergeOperator, config map[string]interface{}) (store.KVStore, error) {
	path, ok := config["path"].(string)
	if !ok {
		return nil, fmt.Errorf("must specify path")
	}
	if path == "" {
		return nil, os.ErrInvalid
	}
	opts := badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path
	sync, _ := config["sync"].(bool)
	opts.SyncWrites = sync
	ro, ok := config["read_only"].(bool)
	if ok {
		opts.ReadOnly = ro
	}

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	rv := Store{
		path: path,
		db:   db,
		mo: mo,
	}
	return &rv, nil
}


func (s *Store) Writer() (store.KVWriter, error) {
	return NewWriter(s.db, s.mo), nil
}

// Reader returns a KVReader which can be used to
// read data from the KVStore.  If a reader cannot
// be obtained a non-nil error is returned.
func(s *Store) Reader() (store.KVReader, error) {
	tx := s.db.NewTransaction(false)
	return NewReader(tx), nil
}

func (bs *Store) Close() error {
	if bs == nil {
		return nil
	}
	return bs.db.Close()
}

func init() {
	registry.RegisterKVStore(Name, New)
}
