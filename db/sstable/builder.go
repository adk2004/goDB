package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/adk2004/goDB/db/types"
)

const magicNumber uint64 = 0x474F444246494C45 // "GODBFILE" for footer
const footerSize = 16                         // 8 bytes for index offset and 8 bytes for magic number

func WriteToSStable(dir string, entries []types.Entry) (SSTable, error) {
	// Write the data entries from entries list
	// write the index entries and then write the footer
	if len(entries) == 0 {
		return nil, fmt.Errorf("sstable: cannot write empty entries")
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("sstable: error occured while creating the sstable file %w", err)
	}
	filename := fmt.Sprintf("sstable-%d.sst", time.Now().UnixMilli())
	tmpPath := filepath.Join(dir, filename+".tmp")
	finalPath := filepath.Join(dir, filename)
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("sstable: failed to open fd for the given path %w", err)
	}
	var (
		offset  int64
		idx     = make([]types.IndexEntry, 0)
		scratch [8]byte
	)
	// writing data block
	for _, e := range entries {
		idx = append(idx, types.IndexEntry{Key: e.Key, Offset: offset})
		keyB := []byte(e.Key)
		binary.BigEndian.PutUint32(scratch[:4], uint32(len(keyB)))
		if _, err := file.Write(scratch[:4]); err != nil {
			return nil, abortWrite(file, tmpPath, err)
		}
		offset += 4
		if _, err := file.Write(keyB); err != nil {
			return nil, abortWrite(file, tmpPath, err)
		}
		offset += int64(len(keyB))
		// tombstone handling : tombstone byte is 1
		if e.Value == nil {
			if _, err := file.Write([]byte{1}); err != nil {
				return nil, abortWrite(file, tmpPath, err)
			}
			offset++
			continue
		}
		// liveByte is 0
		if _, err := file.Write([]byte{0}); err != nil {
			return nil, abortWrite(file, tmpPath, err)
		}
		offset++
		binary.BigEndian.PutUint32(scratch[:4], uint32(len(e.Value)))
		if _, err := file.Write(scratch[:4]); err != nil {
			return nil, abortWrite(file, tmpPath, err)
		}
		offset += 4
		if _, err := file.Write(e.Value); err != nil {
			return nil, abortWrite(file, tmpPath, err)
		}
		offset += int64(len(e.Value))
	}
	// writing index block
	indexStart := offset
	for _, ie := range idx {
		kB := []byte(ie.Key)
		binary.BigEndian.PutUint32(scratch[:4], uint32(len(kB)))
		if _, err := file.Write(scratch[:4]); err != nil {
			return nil, abortWrite(file, tmpPath, err)
		}
		if _, err := file.Write(kB); err != nil {
			return nil, abortWrite(file, tmpPath, err)
		}
		binary.BigEndian.PutUint64(scratch[:8], uint64(ie.Offset))
		if _, err := file.Write(scratch[:8]); err != nil {
			return nil, abortWrite(file, tmpPath, err)
		}
	}
	// writing footer
	binary.BigEndian.PutUint64(scratch[:8], uint64(indexStart))
	if _, err := file.Write(scratch[:8]); err != nil {
		return nil, abortWrite(file, tmpPath, err)
	}
	binary.BigEndian.PutUint64(scratch[:8], uint64(magicNumber))
	if _, err := file.Write(scratch[:8]); err != nil {
		return nil, abortWrite(file, tmpPath, err)
	}
	if err := file.Sync(); err != nil {
		return nil, abortWrite(file, tmpPath, err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("sstable : file close error %w", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("sstable: error occured while renaming the sstable file %w", err)
	}
	return OpenSStable(finalPath)
}

func abortWrite(file *os.File, tmpPath string, err error) error {
	_ = file.Close()
	_ = os.Remove(tmpPath)
	return fmt.Errorf("sstable: write aborted %w", err)
}

func OpenSStable(path string) (SSTable, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: error occured while opening the sstable file %q,  %w", path, err)
	}
	defer file.Close()
	if _, err := file.Seek(-footerSize, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("sstable: file seeking %w", err)
	}
	var footer [footerSize]byte
	if _, err := io.ReadFull(file, footer[:]); err != nil {
		return nil, fmt.Errorf("sstable: footer read %w", err)
	}
	indexStart := binary.BigEndian.Uint64(footer[:8])
	if magic := binary.BigEndian.Uint64(footer[8:]); magic != magicNumber {
		return nil, fmt.Errorf("sstable: corrupt file %q, invalid magic number %w", path, err)
	}
	// read index block
	if _, err := file.Seek(int64(indexStart), io.SeekStart); err != nil {
		return nil, fmt.Errorf("sstable: file seeking %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("sstable: file stat %w", err)
	}
	indexEnd := info.Size() - footerSize
	var (
		scratch [8]byte
		index   []types.IndexEntry
	)
	for {
		cur, _ := file.Seek(0, io.SeekCurrent)
		if cur >= indexEnd {
			break
		}
		if _, err := io.ReadFull(file, scratch[:4]); err != nil {
			return nil, fmt.Errorf("sstable : error reading idx key length %w", err)
		}
		keyB := make([]byte, binary.BigEndian.Uint32(scratch[:4]))
		if _, err := io.ReadFull(file, keyB); err != nil {
			return nil, fmt.Errorf("sstable : error while reading key %w", err)
		}
		if _, err := io.ReadFull(file, scratch[:8]); err != nil {
			return nil, fmt.Errorf("sstable : error while reading OFFSET %w", err)
		}
		index = append(index, types.IndexEntry{
			Key:    types.Key(keyB),
			Offset: int64(binary.BigEndian.Uint64(scratch[:8])),
		})
	}
	return &sstable{filepath: path, index: index}, nil
}
