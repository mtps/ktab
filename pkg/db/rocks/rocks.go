package rocks

import (
	"errors"
	"github.com/mtps/ktab/pkg/db"
	"github.com/tecbot/gorocksdb"
)

type rocksDB struct {
	handle *gorocksdb.DB
}

func (r *rocksDB) Get(key []byte) ([]byte, error) {
	return r.handle.GetBytes(gorocksdb.NewDefaultReadOptions(), key)
}

func (r *rocksDB) Put(key []byte, value []byte) error {
	return r.handle.Put(gorocksdb.NewDefaultWriteOptions(), key, value)
}

func (r *rocksDB) Close() {
	r.handle.Close()
}

func (r *rocksDB) NewIterator(onEach func(k, v []byte) error) error {
	it := r.handle.NewIterator(gorocksdb.NewDefaultReadOptions())
	if it == nil {
		return errors.New("rocksdb iterator is nil")
	}
	defer it.Close()
	for ; it.Valid(); it.Next() {
		if it.Err() != nil {
			return it.Err()
		}
		key := it.Key().Data()
		bytes := it.Value().Data()
		if err := onEach(key, bytes); err != nil {
			return err
		}
	}
	return nil
}

func NewRocksDB(path string) (db.DB, error) {
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	d, err := gorocksdb.OpenDb(opts, path)
	if err != nil {
		return nil, err
	}

	return &rocksDB{
		handle: d,
	}, nil
}
