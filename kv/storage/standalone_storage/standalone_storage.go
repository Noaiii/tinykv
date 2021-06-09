package standalone_storage

import (
	"sync"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"

	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	sync.RWMutex
	db      *badger.DB
	eng     *engine_util.Engines
	closed  bool
	closeCh chan struct{}
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	kv := engine_util.CreateDB(conf.DBPath, false)
	raft := engine_util.CreateDB(conf.DBPath, true)
	eng := engine_util.NewEngines(kv, raft, conf.DBPath, conf.DBPath)
	s := &StandAloneStorage{
		closeCh: make(chan struct{}),
		eng:     eng,
	}
	return s
}

func NewStandAloneStorage1(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	s := &StandAloneStorage{
		closeCh: make(chan struct{}),
	}
	dir := conf.DBPath
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	s.db = db
	s.closed = false
	return s
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	for {
		select {
		case <-s.closeCh:
			s.closeDB()

		}
	}
	return nil
}

func (s *StandAloneStorage) closeDB() {
	s.Lock()
	defer s.Unlock()
	s.db.Close()
	s.closed = true
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.eng.Close()
	if err != nil {
		log.Errorf("[stl storage] close err: %s", err)
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return nil, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	wb := engine_util.WriteBatch{}
	for _, b := range batch {
		switch b.Data.(type) {
		case storage.Put:
			wb.SetCF(b.Cf(), b.Key(), b.Value())
		case storage.Delete:
			wb.DeleteCF(b.Cf(), b.Key())
		}
	}
	err := wb.WriteToDB(s.eng.Kv)
	if err != nil {
		log.Errorf("[stl storage] set err: %s", err)
		return err
	}
	return nil
}

// func (s *StandAloneStorage) Write1(ctx *kvrpcpb.Context, batch []storage.Modify) error {
// 	// Your Code Here (1).
// 	txn := db.NewTransaction(true) // Read-write txn
// 	for _, m := range batch {
// 		switch data := m.Data.(type) {
// 		case Put:
// 			// item := memItem{data.Key, data.Value, false}
// 			err = txn.Set(data.Key, data.Value)
// 			if err != nil {
// 				log.Errorf("set err: %s", err)
// 				return err
// 			}
// 		case Delete:
// 			item := memItem{key: data.Key}
// 			err = txn.Delete(data.Key)
// 			if err != nil {
// 				log.Errorf("delete err: %s", err)
// 				return err
// 			}
// 		}
// 	}
// 	err = txn.Commit()
// 	if err != nil {
// 		log.Errorf("commit err: %s", err)
// 		return err
// 	}
// 	return nil
// }

type saReader struct {
	inner     *StandAloneStorage
	iterCount int
}
