package sstable

import (
	"encoding/binary"
	"io"
	"os"
	"sort"
	"sync"

	bloomfilter "github.com/adk2004/goDB/db/bloomFilter"
	"github.com/adk2004/goDB/db/types"
)

type SSTable interface {
	Get(key types.Key) (types.Value, bool, error)
	GetAllEntries() ([]types.Entry, error)
	Path() string
	Delete() error
}

type sstable struct {
	filepath string
	bf       bloomfilter.BloomFilter
	index    []types.IndexEntry
	mu       sync.RWMutex
}

func (s *sstable) Get(key types.Key) (types.Value, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if(!s.bf.Contains(key)) {
		return nil, false, nil
	}
	idx := sort.Search(len(s.index), func(i int) bool {
		return s.index[i].Key >= key
	})
	if idx >= len(s.index) || s.index[idx].Key != key {
		return nil, false, nil
	}
	entry, err := s.readEntryAt(s.index[idx].Offset)
	if err != nil {
		return nil, false, err
	}
	if entry.Value == nil {
		return nil, true, nil // tombstone case deleted key
	}
	return entry.Value, true, nil
}

func (s *sstable) GetAllEntries() ([]types.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entries := make([]types.Entry, 0, len(s.index))
	for _, idx := range s.index {
		entry, err := s.readEntryAt(idx.Offset)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (s *sstable) readEntryAt(offset int64) (types.Entry, error) {
	file, err := os.Open(s.filepath)
	if err != nil {
		return types.Entry{}, err
	}
	defer func() { _ = file.Close() }()
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return types.Entry{}, err
	}
	var scratch [4]byte
	if _, err := io.ReadFull(file, scratch[:4]); err != nil {
		return types.Entry{}, err
	}
	keyBuff := make([]byte, binary.BigEndian.Uint32(scratch[:4]))
	if _, err := io.ReadFull(file, keyBuff); err != nil {
		return types.Entry{}, err
	}
	// read flag
	var flag [1]byte
	if _, err := io.ReadFull(file, flag[:]); err != nil {
		return types.Entry{}, err
	}
	if flag[0] == 1 { // condition for checking tombtsone
		return types.Entry{Key: types.Key(keyBuff), Value: nil}, nil
	}
	if _, err := io.ReadFull(file, scratch[:4]); err != nil {
		return types.Entry{}, err
	}
	val := make([]byte, binary.BigEndian.Uint32(scratch[:4]))
	if _, err := io.ReadFull(file, val); err != nil {
		return types.Entry{}, err
	}
	return types.Entry{Key: types.Key(keyBuff), Value: val}, nil
}

func (s *sstable) Path() string {
	return s.filepath
}
func (s *sstable) Delete() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := os.Remove(s.filepath)
	return err
}
