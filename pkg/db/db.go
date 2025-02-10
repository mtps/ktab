package db

type DB interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	NewIterator(func(k, v []byte) error) error
	Count() int
	Close()
}
