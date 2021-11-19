package db

type DB interface {
	Put(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)

	NewIterator(start, end []byte) Iterator

	Close() error
	Size() uint64
}

type Iterator interface {
	Valid() bool
	Next() bool
	Close() error
	Error() error

	Key() []byte
	Value() []byte

}