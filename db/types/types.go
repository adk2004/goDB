package types

type Key = string

type Value = []byte

type Entry struct {
	Key   Key
	Value Value
}

type IndexEntry struct {
	Key    Key
	Offset int64
}
