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
	// db     *badger.DB
	eng    *engine_util.Engines
	closed bool
	stopc  chan struct{}
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	kv := engine_util.CreateDB(conf.DBPath, false)
	var eng *engine_util.Engines
	if conf.Raft {
		// raftPath := path.Join(conf.DBPath, "/tmp/raft/")
		raftPath := "/tmp/raft"
		raft := engine_util.CreateDB(raftPath, true)
		eng = engine_util.NewEngines(kv, raft, conf.DBPath, raftPath)
	} else {
		eng = engine_util.NewEngines(kv, nil, conf.DBPath, "")
	}
	s := &StandAloneStorage{
		stopc: make(chan struct{}),
		eng:   eng,
	}
	return s
}

// func NewStandAloneStorage1(conf *config.Config) *StandAloneStorage {
// 	// Your Code Here (1).
// 	s := &StandAloneStorage{
// 		stopc: make(chan struct{}),
// 	}
// 	dir := conf.DBPath
// 	opts := badger.DefaultOptions
// 	opts.Dir = dir
// 	opts.ValueDir = dir
// 	db, err := badger.Open(opts)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	s.db = db
// 	s.closed = false
// 	return s
// }

func (s *StandAloneStorage) run() error {
	for {
		select {
		case <-s.stopc:
			err := s.closeDB()
			if err != nil {
				log.Error(err)
			}
			return err

		}
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	go s.run()
	return nil
}

func (s *StandAloneStorage) closeDB() error {
	log.Info("closeDB()")
	s.Lock()
	defer s.Unlock()
	err := s.eng.Close()
	if err != nil {
		log.Errorf("[stl storage] close err: %s", err)
		return err
	}
	s.closed = true
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.stopc <- struct{}{}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	sr, err := NewStlReader(s.eng.Kv)
	if err != nil {
		return nil, err
	}
	return sr, nil
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

type StlReader struct {
	db   *badger.DB
	txn  *badger.Txn
	iter *engine_util.BadgerIterator
}

func NewStlReader(stl *badger.DB) (*StlReader, error) {
	return &StlReader{db: stl}, nil
}

func (sr *StlReader) GetCF(cf string, key []byte) ([]byte, error) {
	v, err := engine_util.GetCF(sr.db, cf, key)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (sr *StlReader) IterCF(cf string) engine_util.DBIterator {
	// var iter engine_util.DBIterator
	if sr.iter == nil {
		txn := sr.getTxn()
		sr.iter = engine_util.NewCFIterator(cf, txn)
	}
	return sr.iter
}

func (sr *StlReader) getTxn() *badger.Txn {
	if sr.txn == nil {
		sr.txn = sr.db.NewTransaction(false)
	}
	return sr.txn
}

func (sr *StlReader) Close() {
	if sr.txn != nil {
		sr.txn.Discard()
	}
	if sr.iter != nil {
		sr.Close()
	}
}
