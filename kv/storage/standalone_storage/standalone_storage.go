package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"log"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	var err error
	// my note: open local db
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	return &StandAloneStorage{db: db}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return reader{s.db.NewTransaction(false)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	txn := s.db.NewTransaction(true)
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			err := txn.Set(engine_util.KeyWithCF(m.Cf(), m.Key()), m.Value())
			if err != nil {
				return err
			}
		case storage.Delete:
			err := txn.Delete(engine_util.KeyWithCF(m.Cf(), m.Key()))
			if err != nil {
				return err
			}
		}
	}
	err := txn.Commit()
	if err != nil {
		return err
	}
	txn.Discard()
	return nil
}

// my note: impl of StorageReader
type reader struct {
	txn *badger.Txn
}

func (r reader) GetCF(cf string, key []byte) ([]byte, error) {
	txn, _ := engine_util.GetCFFromTxn(r.txn, cf, key)
	return txn, nil
}
func (r reader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}
func (r reader) Close() {
	r.txn.Discard()
}
