package engine

import (
	"container/heap"
	"fmt"
	"os"
	"time"
	"sort"

	"github.com/adk2004/goDB/db/sstable"
	"github.com/adk2004/goDB/db/types"
)

// Compaction strategies:
// 1.) STCS (Size-Tiered Compaction Strategy) - used by Cassandra
//     Groups SSTables by similar size and compacts them when a tier has enough tables
// 2.) LCS (Leveled Compaction Strategy) - used by LevelDB/RocksDB
//     Organizes SSTables into levels with size limits

// Configuration constants for STCS
const (
	// MinCompactionThreshold is the minimum number of SSTables in a tier to trigger compaction
	MinCompactionThreshold = 4
	// SizeTierBucketLow is the lower bound ratio for size similarity (0.5 means 50% of average)
	SizeTierBucketLow = 0.5
	// SizeTierBucketHigh is the upper bound ratio for size similarity (1.5 means 150% of average)
	SizeTierBucketHigh = 1.5
)

// sstableInfo holds metadata about an SSTable for compaction decisions
type sstableInfo struct {
	table sstable.SSTable
	size  int64
}

// STCS (Size-Tiered Compaction Strategy)
// SelectSSTablesToCompactSTCS selects SSTables for compaction using Size-Tiered strategy.
// It groups SSTables by similar size and returns a group that has reached the compaction threshold.
// Returns nil if no compaction is needed.
func SelectSSTablesToCompactSTCS(tables []sstable.SSTable) []sstable.SSTable {
	if len(tables) < MinCompactionThreshold {
		return nil
	}

	// Gather size info for each SSTable
	infos := make([]sstableInfo, 0, len(tables))
	for _, t := range tables {
		size, err := getSSTableSize(t.Path())
		if err != nil {
			continue // Skip tables we can't stat
		}
		infos = append(infos, sstableInfo{table: t, size: size})
	}

	if len(infos) < MinCompactionThreshold {
		return nil
	}

	// Sort by size for easier bucketing
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].size < infos[j].size
	})

	// Find buckets of similarly-sized SSTables
	buckets := groupBySize(infos)

	// Find the first bucket that meets the compaction threshold
	for _, bucket := range buckets {
		if len(bucket) >= MinCompactionThreshold {
			result := make([]sstable.SSTable, len(bucket))
			for i, info := range bucket {
				result[i] = info.table
			}
			return result
		}
	}

	return nil
}

// groupBySize groups SSTables into buckets of similar sizes
func groupBySize(infos []sstableInfo) [][]sstableInfo {
	if len(infos) == 0 {
		return nil
	}

	var buckets [][]sstableInfo
	var currentBucket []sstableInfo

	for _, info := range infos {
		if len(currentBucket) == 0 {
			currentBucket = append(currentBucket, info)
			continue
		}

		// Calculate average size of current bucket
		var totalSize int64
		for _, bi := range currentBucket {
			totalSize += bi.size
		}
		avgSize := totalSize / int64(len(currentBucket))

		// Check if this SSTable fits in the current bucket (within size ratio bounds)
		lowBound := int64(float64(avgSize) * SizeTierBucketLow)
		highBound := int64(float64(avgSize) * SizeTierBucketHigh)

		if info.size >= lowBound && info.size <= highBound {
			currentBucket = append(currentBucket, info)
		} else {
			// Start a new bucket
			buckets = append(buckets, currentBucket)
			currentBucket = []sstableInfo{info}
		}
	}

	// Don't forget the last bucket
	if len(currentBucket) > 0 {
		buckets = append(buckets, currentBucket)
	}

	return buckets
}

// getSSTableSize returns the file size of an SSTable
func getSSTableSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// K-Way Merge Implementation

// mergeEntry represents an entry with its source SSTable index for the merge heap
type mergeEntry struct {
	entry    types.Entry
	tableIdx int // Index of the source SSTable (newer tables have higher indices)
	entryIdx int // Current index within the source SSTable's entries
}

// mergeHeap implements heap.Interface for k-way merge
type mergeHeap []mergeEntry

func (h mergeHeap) Len() int { return len(h) }

func (h mergeHeap) Less(i, j int) bool {
	// Primary sort by key
	if h[i].entry.Key != h[j].entry.Key {
		return h[i].entry.Key < h[j].entry.Key
	}
	// For same key, prefer higher tableIdx (newer SSTable)
	return h[i].tableIdx > h[j].tableIdx
}

func (h mergeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *mergeHeap) Push(x any) {
	*h = append(*h, x.(mergeEntry))
}

func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// KWayMerge merges multiple SSTables into a single sorted list of entries.
// It performs a k-way merge using a min-heap, keeping only the newest value for each key.
// Tables is expected to be ordered from oldest to newest.
// Tombstones (nil values) from the newest SSTable are preserved to handle deletes.
func KWayMerge(tables []sstable.SSTable) ([]types.Entry, error) {
	if len(tables) == 0 {
		return nil, nil
	}

	// Load all entries from each SSTable
	allEntries := make([][]types.Entry, len(tables))
	for i, t := range tables {
		entries, err := t.GetAllEntries()
		if err != nil {
			return nil, fmt.Errorf("compaction: failed to read sstable %s: %w", t.Path(), err)
		}
		allEntries[i] = entries
	}

	// Initialize the heap with the first entry from each SSTable
	h := &mergeHeap{}
	heap.Init(h)

	for i, entries := range allEntries {
		if len(entries) > 0 {
			heap.Push(h, mergeEntry{
				entry:    entries[0],
				tableIdx: i,
				entryIdx: 0,
			})
		}
	}

	// Perform k-way merge
	var result []types.Entry
	var lastKey types.Key
	var lastValue types.Value
	firstKey := true

	for h.Len() > 0 {
		// Pop the smallest entry
		me := heap.Pop(h).(mergeEntry)

		if firstKey {
			lastKey = me.entry.Key
			lastValue = me.entry.Value
			firstKey = false
		} else if me.entry.Key != lastKey {
			// New key encountered, flush the previous key
			result = append(result, types.Entry{Key: lastKey, Value: lastValue})
			lastKey = me.entry.Key
			lastValue = me.entry.Value
		}
		// If same key, the heap ordering ensures we already have the newest value
		// (higher tableIdx comes first due to our Less function), so we skip duplicates

		// Push the next entry from the same SSTable
		nextIdx := me.entryIdx + 1
		if nextIdx < len(allEntries[me.tableIdx]) {
			heap.Push(h, mergeEntry{
				entry:    allEntries[me.tableIdx][nextIdx],
				tableIdx: me.tableIdx,
				entryIdx: nextIdx,
			})
		}
	}

	//  last key
	if !firstKey {
		result = append(result, types.Entry{Key: lastKey, Value: lastValue})
	}

	return result, nil
}

// Compaction Execution

// CompactSSTables compacts the given SSTables into a single new SSTable.
// It merges all entries using k-way merge and writes the result to a new SSTable.
// The old SSTables are deleted after successful compaction.
// Returns the new SSTable on success.
func CompactSSTables(dataDir string, tables []sstable.SSTable) (sstable.SSTable, error) {
	if len(tables) == 0 {
		return nil, fmt.Errorf("compaction: no tables to compact")
	}

	if len(tables) == 1 {
		return tables[0], nil // Nothing to compact
	}

	// Merge all entries
	mergedEntries, err := KWayMerge(tables)
	if err != nil {
		return nil, fmt.Errorf("compaction: merge failed: %w", err)
	}

	// Remove tombstones for fully compacted data (no older SSTables could have the key)
	// Note: In a full LSM implementation, you'd only remove tombstones at the lowest level
	// For STCS, we keep tombstones since there may be older SSTables
	compactedEntries := mergedEntries

	if len(compactedEntries) == 0 {
		// All entries were tombstones, delete old SSTables and return nil
		for _, t := range tables {
			_ = t.Delete()
		}
		return nil, nil
	}

	// Write merged entries to a new SSTable
	newTable, err := sstable.WriteToSStable(dataDir, compactedEntries)
	if err != nil {
		return nil, fmt.Errorf("compaction: failed to write new sstable: %w", err)
	}

	// Delete old SSTables
	for _, t := range tables {
		if err := t.Delete(); err != nil {
			// Log error but continue - the new SSTable is already created
			fmt.Printf("compaction: warning: failed to delete old sstable %s: %v\n", t.Path(), err)
		}
	}

	return newTable, nil
}

// RunCompaction performs a compaction cycle on the engine's SSTables.
// It selects SSTables using STCS and compacts them if a tier reaches the threshold.
// This method should be called periodically or when the number of SSTables grows.
func (eng *engine) runCompaction() error {
	eng.mu.RLock()
	defer eng.mu.RUnlock()
	// Select SSTables for compaction using STCS
	tablesToCompact := SelectSSTablesToCompactSTCS(eng.sstables)
	if tablesToCompact == nil {
		return nil // No compaction needed
	}

	// Create a set of tables being compacted for quick lookup
	compactingSet := make(map[string]bool)
	for _, t := range tablesToCompact {
		compactingSet[t.Path()] = true
	}

	// Perform compaction
	newTable, err := CompactSSTables(eng.datadir, tablesToCompact)
	if err != nil {
		return fmt.Errorf("compaction failed: %w", err)
	}

	// Update the sstables slice: remove compacted tables, add new one
	newSSTables := make([]sstable.SSTable, 0, len(eng.sstables)-len(tablesToCompact)+1)
	for _, t := range eng.sstables {
		if !compactingSet[t.Path()] {
			newSSTables = append(newSSTables, t)
		}
	}
	if newTable != nil {
		newSSTables = append(newSSTables, newTable)
	}

	// Sort by path to maintain oldest-to-newest order
	sort.Slice(newSSTables, func(i, j int) bool {
		return newSSTables[i].Path() < newSSTables[j].Path()
	})

	eng.sstables = newSSTables
	return nil
}

// ShouldRunCompaction returns true if compaction should be triggered.
// This can be used to decide whether to run compaction based on current state.
func (eng *engine) shouldRunCompaction() bool {
	eng.mu.RLock()
	defer eng.mu.RUnlock()
	return len(eng.sstables) >= MinCompactionThreshold
}

func (eng *engine) backgroundCompactionLoop() {
	for {
		if eng.shouldRunCompaction() {
			if err := eng.runCompaction(); err != nil {
				fmt.Printf("background compaction error: %v\n", err)
			}
		}
		time.Sleep(time.Minute)
	}
}