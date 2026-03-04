package engine

import (
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/adk2004/goDB/db/memtable"
	"github.com/adk2004/goDB/db/sstable"
	"github.com/adk2004/goDB/db/types"
	"github.com/adk2004/goDB/db/wal"
)

type Engine interface {
	Get(key types.Key) (types.Value, error)
	Put(key types.Key, value types.Value) error
	Delete(key types.Key) error
	List() []types.Key
	Close()
	Clear() error
}

type engine struct {
	wal wal.WAL
	memtable memtable.Memtable
	sstables []sstable.SSTable
	datadir string
	mu sync.RWMutex
	stopChan chan struct{}
}

func NewEngine(dataDir string, size int) (Engine, error) {
	wal, err := wal.NewWAL(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize WAL: %w", err)
	}
	mem := memtable.NewMemtable(size)
	sstables, err := loadSSTables(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to load sstables: %w", err)
	}

	eng :=  &engine{
		wal: wal,
		memtable: mem,
		sstables: sstables,
		datadir: dataDir,
		stopChan: make(chan struct{}),
	}
	if err := eng.replayWAL(); err != nil {
		return nil, fmt.Errorf("failed to replay WAL: %w", err)
	}
	go eng.backgroundFlush()
	return eng, nil
}

func loadSSTables(dataDir string) ([]sstable.SSTable, error) {
    pattern := filepath.Join(dataDir, "sstable-*.sst")
    files, err := filepath.Glob(pattern)
    if err != nil {
        return nil, fmt.Errorf("failed to scan sstables: %w", err)
    }
    sort.Strings(files) // lexo sorting gives sstables in oldest to newest order
    var sstables []sstable.SSTable
    for _, f := range files {
        sst, err := sstable.OpenSStable(f)
        if err != nil {
            return nil, fmt.Errorf("failed to open sstable %s: %w", f, err)
        }
        sstables = append(sstables, sst)
    }
    return sstables, nil
}

func (eng *engine) Get(key types.Key) (types.Value, error) {
	eng.mu.RLock()
	defer eng.mu.RUnlock()
	// first search memtable
	val, ok := eng.memtable.Get(key)
	if ok {
		if val == nil {
			return nil, fmt.Errorf("Key Deleted")
		}
		return val, nil
	}
	// serach sstables
	for i:= len(eng.sstables) - 1; i >= 0; i-- {
		val, found, err := eng.sstables[i].Get(key)
		if err != nil {
			return nil, err
		}
		if found {
			if val == nil {
				return nil, fmt.Errorf("Key deleted") // tombstone case
			}
			return val, nil
		}
	}
	return nil, fmt.Errorf("Key not found")
}

func (eng *engine) List() []types.Key{
	eng.mu.RLock()
	defer eng.mu.RUnlock()
	keySet := make(map[types.Key]bool)
	// we traverse sstables in oldest to newest order, so newer key values will override older ones in case of duplicates
	for _, sst := range eng.sstables {
		entries, err := sst.GetAllEntries()
		if err != nil {
			fmt.Printf("Error reading sstable entries: %v\n", err)
			continue
		}
		for _, entry := range entries {
			if entry.Value != nil {
				keySet[entry.Key] = true
			} else {
				keySet[entry.Key] = false
			}
		}
	}
	kl := make([]types.Key, 0, len(keySet))
	for k := range keySet {
		if keySet[k] {
			kl = append(kl, k)
		}
	}
	return kl
}

func (eng *engine) Put(key types.Key, value types.Value) error {
	eng.mu.Lock()
	defer eng.mu.Unlock()
	if err := eng.wal.LogInsert(key, value); err != nil {
		return fmt.Errorf("Put Error : %w", err)
	}
	eng.memtable.Put(key, value)
	return nil
}

func (eng *engine) Delete(key types.Key) error{
	eng.mu.Lock()
	defer eng.mu.Unlock()
	if err := eng.wal.LogDelete(key); err != nil {
		return fmt.Errorf("Delete Error : %w", err)
	}
	eng.memtable.Delete(key)
	return nil
}

func (eng *engine) Close() {
	close(eng.stopChan)
	eng.wal.Close()
}

func (eng *engine) Clear() error{
	eng.mu.Lock()
	defer eng.mu.Unlock()

	if err:= eng.wal.Clear() ;err!= nil {
		return fmt.Errorf("Clear Error : %w", err)
	}
	eng.memtable.Clear()
	eng.sstables = make([]sstable.SSTable, 0)
	return nil
}

func (eng *engine) replayWAL() error {
	insertHandler := func(key types.Key, val types.Value) {
		eng.memtable.Put(key, val)
	}
	deleteHanlder := func(key types.Key){
		eng.memtable.Delete(key)
	}
	return eng.wal.Replay(insertHandler, deleteHanlder)
}

func (eng *engine) flushMemtable() error {
	memEntries := eng.memtable.GetAllEntries()
	sstable, err := sstable.WriteToSStable(eng.datadir, memEntries)
	if err != nil {
		return fmt.Errorf("flush error : %w", err)
	}
	eng.sstables = append(eng.sstables, sstable)
	if err := eng.wal.Clear(); err!=  nil {
		return fmt.Errorf("wal clear err: %w", err)
	}
	eng.memtable.Clear()
	return nil
}

func (eng *engine) backgroundFlush() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <- ticker.C:
			eng.mu.Lock()
			if eng.memtable.GetSize() > 0 {
				if err := eng.flushMemtable(); err != nil {
					fmt.Printf("Error flushing memtable: %v\n", err)
				}
			}
			eng.mu.Unlock()
		case <- eng.stopChan:
			return
		}
	}
}