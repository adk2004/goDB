package wal

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	constants "github.com/adk2004/goDB/db/contants"
	"github.com/adk2004/goDB/db/types"
)

const (
	OpInsert = "INSERT"
	OpDelete = "DELETE"
)


type LogEntry struct {
	Operation string `json:"op"`
	Key string	`json:"key"`
	Value []byte `json:"value,omitempty"`
	Timestamp int64 `json:"timestamp"`
}

type WAL interface {
	AppendLog(entry LogEntry) error
	LogInsert(key types.Key, value types.Value) error
	LogDelete(key types.Key) error
	Replay(insertHandler func(key types.Key, value types.Value) error, deleteHandler func(key types.Key) error) error
	Clear() error
}

type wal struct {
	filepath string
	file *os.File
	mu sync.Mutex
}

func NewWAL(dataDir string) (WAL, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %v", err)
	}
	filepath := filepath.Join(dataDir, constants.WALFilename)
	file, err := os.OpenFile(filepath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %v", err)
	}
	return &wal{
		filepath: filepath,
		file: file,
	}, nil
}

func (w *wal) LogInsert(key types.Key, value types.Value) error {
	entry := LogEntry{
		Operation: OpInsert,
		Key: key,
		Value: value,
		Timestamp: time.Now().UnixMilli(),
	}
	return w.AppendLog(entry)
}

func (w *wal) LogDelete(key types.Key) error {
	entry := LogEntry{
		Operation: OpDelete,
		Key: key,
		Timestamp: time.Now().UnixMilli(),
	}
	return w.AppendLog(entry)
}

func (w *wal) AppendLog(entry LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := json.Marshal(entry)
	if err!= nil {
		return fmt.Errorf("failed to marshal log entry %w", err)
	}
	length := uint32(len(data))
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, length)
	if _, err := w.file.Write(lengthBytes); err != nil {
		return fmt.Errorf("failed to write length of the entry to WAL: %w", err)
	}
	if _, err := w.file.Write(data); err != nil {
		return fmt.Errorf("failed to write data to WAL: %w", err)
	}
	return w.file.Sync()
}

func (w *wal) Replay(insertHandler func(key types.Key, value types.Value) error, deleteHandler func(key types.Key) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return nil
}

func (w *wal) Clear() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		_ = w.file.Close()
		w.file = nil
	}
	if err := os.Remove(w.filepath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove WAL file: %w", err)
	}
	file, err := os.OpenFile(w.filepath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to create new WAL file: %w", err)
	}
	w.file = file
	return nil
}