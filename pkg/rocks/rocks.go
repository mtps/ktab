package rocks

import (
	"github.com/tecbot/gorocksdb"
)

func NewRocksDB(path string) (*gorocksdb.DB, error) {
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	return gorocksdb.OpenDb(opts, path)
}


