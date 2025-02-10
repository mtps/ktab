package bolt

import (
	"github.com/mtps/ktab/pkg/db"
	bb "go.etcd.io/bbolt"
	"log"
)

var boltDBBucket = []byte("default")
var nilKey = []byte("__0xNILL")

type boltDB struct {
	handle *bb.DB
	bucket []byte
}

func NewBoltDB(path string) (db.DB, error) {
	log.Printf("Opening Bolt DB at %s", path)
	db, err := bb.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
	tx, err := db.Begin(true)
	if err != nil {
		return nil, err
	}
	_, err = tx.CreateBucketIfNotExists(boltDBBucket)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &boltDB{
		handle: db,
		bucket: boltDBBucket,
	}, nil
}

func (b *boltDB) Put(key, value []byte) error {
	if key == nil {
		key = nilKey
	}

	return b.handle.Update(func(tx *bb.Tx) error {
		bk := tx.Bucket(b.bucket)
		return bk.Put(key, value)
	})
}

func (b *boltDB) Get(key []byte) ([]byte, error) {
	if key == nil {
		key = nilKey
	}
	var value []byte
	err := b.handle.View(func(tx *bb.Tx) error {
		buk := tx.Bucket(b.bucket)
		v := buk.Get(key)
		value = append(v[:0:0], v...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (b *boltDB) Count() int {
	var count int
	b.handle.View(func(tx *bb.Tx) error {
		buk := tx.Bucket(b.bucket)
		count = buk.Stats().KeyN
		return nil
	})
	return count
}

func (b *boltDB) NewIterator(onEach func(k, v []byte) error) error {
	err := b.handle.View(func(tx *bb.Tx) error {
		buk := tx.Bucket(b.bucket)
		if err := buk.ForEach(onEach); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (b *boltDB) Close() {
	err := b.handle.Close()
	if err != nil {
		panic(err)
	}
}
