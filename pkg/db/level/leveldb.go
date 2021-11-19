package level

import (
	"github.com/mtps/ktab/pkg/db"
)

type levelDB struct {

}

var _ db.DB = (*levelDB)(nil)

func NewLevelDB(path string) (db.DB, error) {
	panic("todo()")
}

func (l *levelDB) Put(key []byte, value []byte) error {
	panic("implement me")
}

func (l *levelDB) Get(key []byte) ([]byte, error) {
	panic("implement me")
}

func (l *levelDB) Close() error {
	panic("implement me")
}

func (l *levelDB) NewIterator(start, end []byte) db.Iterator {
	panic("implement me")
}

func (l *levelDB) Size() int64 {
	panic("implement me")
}

func init() {
	db.Register("level", NewLevelDB)
}