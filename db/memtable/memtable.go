package memtable

import (
	"sort"
	"sync"

	"github.com/adk2004/goDB/db/types"
)

// MemTable can be a hashMap or list of Entries or a skipList (as in most real implementations)
// the choice of Data structure can be debated but i have decided to use hashMap for simplicity

type Memtable interface {
	Put(key string, value []byte)
	Get(key string) ([]byte, bool)
	Delete(key string)
	GetAllEntries() []types.Entry
	isFull() bool
	GetSize() int
	Clear()
}

type memtable struct {
	entries map[types.Key]types.Value
	size    int
	mu      sync.RWMutex
}

func NewMemtable(size int) Memtable {
	return &memtable{
		entries: make(map[types.Key]types.Value),
		size:    size,
	}
}

func (m *memtable) Put(key types.Key, value types.Value) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries[key] = value
}

func (m *memtable) Get(key types.Key) (types.Value, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.entries[key]
	return val, ok
}

func (m *memtable) Delete(key types.Key) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries[key] = nil // tombstone
}

// flushing is slow for a hashMap O(nlogn)
func (m *memtable) GetAllEntries() []types.Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entryList := make([]types.Entry, 0, len(m.entries))
	for k, v := range m.entries {
		entryList = append(entryList, types.Entry{Key: k, Value: v})
	}
	sort.Slice(entryList, func(i, j int) bool {
		return entryList[i].Key < entryList[j].Key
	})
	return entryList
}

func (m *memtable) isFull() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.GetSize() >= m.size
}

func (m *memtable) GetSize() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.entries)
}

func (m *memtable) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = make(map[types.Key]types.Value)
}
