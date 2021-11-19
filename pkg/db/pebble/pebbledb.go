package pebble

import (
	"encoding/binary"
	"github.com/cockroachdb/pebble"
	"github.com/mtps/ktab/pkg/db"
)

var _ db.DB = (*pebbleDB)(nil)

var sizePrefix = []byte("count")
var dataPrefix = []byte("f/")

type pebbleDB struct {
	pdb *pebble.DB
}

func NewPebbleDB(path string) (db.DB, error) {
	opts := &pebble.Options{}
	pdb, err := pebble.Open(path, opts)
	if err != nil {
		return nil, err
	}
	return &pebbleDB{pdb}, nil
}

func (p *pebbleDB) Put(key []byte, value []byte) error {
	err := p.changeCounter(1)
	if err != nil {
		return err
	}
	return p.rawPut(makeKey(key), value)
}

func (p* pebbleDB) rawPut(key []byte, value []byte) error {
	opts := &pebble.WriteOptions{}
	return p.pdb.Set(key, value, opts)
}

func (p *pebbleDB) Get(key []byte) ([]byte, error) {
	return p.rawGet(makeKey(key))
}

func (p *pebbleDB) Close() error {
	return p.pdb.Close()
}

func (p *pebbleDB) NewIterator(start, end []byte) db.Iterator {
	opts := &pebble.IterOptions{}
	opts.LowerBound = makeKey(start)
	opts.UpperBound = makeKey(end)
	it := p.pdb.NewSnapshot().NewIter(opts)
	it.First()
	return it
}

func (p *pebbleDB) rawGet(key []byte) ([]byte, error) {
	value, closer, err := p.pdb.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (p *pebbleDB) changeCounter(n int) error {
	sizeBz, err := p.rawGet(sizePrefix)
	if err != nil {
		return err
	}

	var size uint64 = 0
	if sizeBz != nil {
		size = binary.LittleEndian.Uint64(sizeBz)
	} else {
		sizeBz = make([]byte, 8)
	}

	size += uint64(n)
	binary.LittleEndian.PutUint64(sizeBz, size)
	return p.rawPut(sizePrefix, sizeBz)
}

func (p *pebbleDB) getCounter() uint64 {
	sizeBz, err := p.rawGet(sizePrefix)
	if err != nil {
		panic(err)
	}
	return binary.LittleEndian.Uint64(sizeBz)
}

func (p *pebbleDB) Size() uint64 {
	return p.getCounter()
}

func makeKey(key []byte) []byte {
	var dest []byte
	dest = append(dest, dataPrefix...)
	dest = append(dest, key...)
	return dest
}

func init() {
	db.Register("pebble", NewPebbleDB)
}