package engine

import (
	"container/heap"
	"fmt"
	"os"
	"sort"
	"time"

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

// ------------------------------------------------------------------
// STCS (Size-Tiered Compaction Strategy)
// ------------------------------------------------------------------

// SelectSSTablesToCompactSTCS selects SSTables for compaction using Size-Tiered strategy.
// It groups SSTables by similar size and returns a group that has reached the compaction
// threshold. Returns nil if no compaction is needed.
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

// groupBySize groups SSTables into buckets of similar sizes.
// It uses the median size of each bucket as the reference point to avoid
// boundary drift caused by a running average shifting as elements are added.
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

		// Use the median element of the current bucket as the stable reference
		// to prevent boundary drift from a shifting running average.
		medianIdx := len(currentBucket) / 2
		refSize := currentBucket[medianIdx].size

		lowBound := int64(float64(refSize) * SizeTierBucketLow)
		highBound := int64(float64(refSize) * SizeTierBucketHigh)

		if info.size >= lowBound && info.size <= highBound {
			currentBucket = append(currentBucket, info)
		} else {
			buckets = append(buckets, currentBucket)
			currentBucket = []sstableInfo{info}
		}
	}

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

// ------------------------------------------------------------------
// K-Way Merge Implementation
// ------------------------------------------------------------------

// mergeEntry represents an entry with its source SSTable index for the merge heap
type mergeEntry struct {
	entry    types.Entry
	tableIdx int // Index of the source SSTable; higher = newer
	entryIdx int // Current position within the source SSTable's entry list
}

// mergeHeap implements heap.Interface for k-way merge (min-heap by key, then newest-first).
type mergeHeap []mergeEntry

func (h mergeHeap) Len() int { return len(h) }

func (h mergeHeap) Less(i, j int) bool {
	// Primary sort: ascending by key
	if h[i].entry.Key != h[j].entry.Key {
		return h[i].entry.Key < h[j].entry.Key
	}
	// Tie-break: higher tableIdx (newer SSTable) surfaces first so the first
	// Pop for a given key always yields the authoritative value.
	return h[i].tableIdx > h[j].tableIdx
}

func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *mergeHeap) Push(x any) { *h = append(*h, x.(mergeEntry)) }

func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// KWayMerge merges multiple SSTables into a single sorted list of entries.
//
// Contract:
//   - tables must be ordered oldest → newest (index 0 = oldest).
//   - For duplicate keys the newest SSTable's value wins.
//   - When dropTombstones is true, entries whose Value is nil are omitted from
//     the output. Pass true only when the compaction set covers ALL existing
//     SSTables (i.e. there are no older SSTables that might still hold the key).
//
// The returned slice is sorted by key and ready to be written to a new SSTable.
// The bloom filter for the new SSTable must be built from these entries by the
// caller (WriteToSStable is expected to do this automatically).
func KWayMerge(tables []sstable.SSTable, dropTombstones bool) ([]types.Entry, error) {
	if len(tables) == 0 {
		return nil, nil
	}

	// Load all entries from every SSTable up front.
	allEntries := make([][]types.Entry, len(tables))
	for i, t := range tables {
		entries, err := t.GetAllEntries()
		if err != nil {
			return nil, fmt.Errorf("compaction: failed to read sstable %s: %w", t.Path(), err)
		}
		allEntries[i] = entries
	}

	// Seed the heap with the first entry from each SSTable.
	h := &mergeHeap{}
	heap.Init(h)
	for i, entries := range allEntries {
		if len(entries) > 0 {
			heap.Push(h, mergeEntry{entry: entries[0], tableIdx: i, entryIdx: 0})
		}
	}

	var result []types.Entry
	// seenKey tracks the last key we resolved so duplicates are skipped
	// explicitly rather than implicitly, making the logic easy to follow.
	seenKey := false
	var resolvedKey types.Key

	for h.Len() > 0 {
		me := heap.Pop(h).(mergeEntry)

		// ----------------------------------------------------------------
		// Duplicate handling:
		// Because Less() surfaces higher tableIdx first for equal keys,
		// the very first Pop for a given key gives us the newest value.
		// All subsequent Pops for the same key are stale — we advance
		// their iterators (to keep the heap fed) but discard their values.
		// ----------------------------------------------------------------
		isNewKey := !seenKey || me.entry.Key != resolvedKey

		if isNewKey {
			if seenKey {
				// Commit the previously resolved entry — but only after we
				// know a new key is starting (avoids an off-by-one flush).
				// NOTE: result already has it appended below; see comment *.
			}

			resolvedKey = me.entry.Key
			seenKey = true

			// Append immediately; if dropTombstones is set we skip nil values.
			// (*) We append here (not on the *next* iteration) so we don't
			// need a separate "flush last key" step after the loop.
			if !(dropTombstones && me.entry.Value == nil) {
				result = append(result, me.entry)
			}
		}
		// If !isNewKey we intentionally do nothing with me.entry.Value —
		// the value from the first (newest) Pop is already in result.

		// Advance the iterator for the table this entry came from.
		nextIdx := me.entryIdx + 1
		if nextIdx < len(allEntries[me.tableIdx]) {
			heap.Push(h, mergeEntry{
				entry:    allEntries[me.tableIdx][nextIdx],
				tableIdx: me.tableIdx,
				entryIdx: nextIdx,
			})
		}
	}

	return result, nil
}

// ------------------------------------------------------------------
// Compaction Execution
// ------------------------------------------------------------------

// CompactSSTables compacts the given SSTables into a single new SSTable.
//
//   - tables must be ordered oldest → newest.
//   - totalTableCount is the total number of SSTables in the engine; when it
//     equals len(tables) this is a full compaction and tombstones are dropped.
//   - Old SSTables are deleted only after the new one is durably written.
//
// The new SSTable is written via sstable.WriteToSStable, which is responsible
// for building a fresh bloom filter from the merged entries. No manual bloom
// filter union is required because the compacted entry set is the authoritative
// source of truth — any key absent from it is correctly absent from the filter.
func CompactSSTables(dataDir string, tables []sstable.SSTable, totalTableCount int) (sstable.SSTable, error) {
	if len(tables) == 0 {
		return nil, fmt.Errorf("compaction: no tables to compact")
	}
	if len(tables) == 1 {
		return tables[0], nil // Nothing to do
	}

	// Tombstones can only be safely removed when there are no older SSTables
	// outside the compaction set that might still hold the key.
	isFullCompaction := len(tables) == totalTableCount
	mergedEntries, err := KWayMerge(tables, isFullCompaction)
	if err != nil {
		return nil, fmt.Errorf("compaction: merge failed: %w", err)
	}

	if len(mergedEntries) == 0 {
		// Every entry was a tombstone and we're doing a full compaction:
		// nothing survives, so remove all old SSTables.
		for _, t := range tables {
			if err := t.Delete(); err != nil {
				fmt.Printf("compaction: warning: failed to delete sstable %s: %v\n", t.Path(), err)
			}
		}
		return nil, nil
	}

	// WriteToSStable MUST build a bloom filter from mergedEntries internally.
	// The resulting SSTable's filter will therefore reflect exactly the keys
	// that exist after compaction — no stale keys from deleted SSTables, no
	// missing keys from the merge. This is the correct behaviour because:
	//
	//   • Unioning old bloom filters would retain false-positives for keys
	//     that were overwritten or tombstoned.
	//   • Intersecting them would produce false-negatives.
	//   • Rebuilding from the merged entries is the only sound approach.
	newTable, err := sstable.WriteToSStable(dataDir, mergedEntries)
	if err != nil {
		return nil, fmt.Errorf("compaction: failed to write new sstable: %w", err)
	}

	// Delete old SSTables only after the new file is safely on disk.
	for _, t := range tables {
		if err := t.Delete(); err != nil {
			fmt.Printf("compaction: warning: failed to delete old sstable %s: %v\n", t.Path(), err)
		}
	}

	return newTable, nil
}

// ------------------------------------------------------------------
// Engine-level compaction integration
// ------------------------------------------------------------------

// runCompaction performs one compaction cycle.
// It selects a tier using STCS and compacts it if the threshold is met.
func (eng *engine) runCompaction() error {
	eng.mu.Lock()
	defer eng.mu.Unlock()

	tablesToCompact := SelectSSTablesToCompactSTCS(eng.sstables)
	if tablesToCompact == nil {
		return nil
	}

	totalCount := len(eng.sstables)

	compactingSet := make(map[string]bool, len(tablesToCompact))
	for _, t := range tablesToCompact {
		compactingSet[t.Path()] = true
	}

	newTable, err := CompactSSTables(eng.datadir, tablesToCompact, totalCount)
	if err != nil {
		return fmt.Errorf("compaction failed: %w", err)
	}

	// Rebuild the sstables slice: keep non-compacted tables, append the new one.
	newSSTables := make([]sstable.SSTable, 0, len(eng.sstables)-len(tablesToCompact)+1)
	for _, t := range eng.sstables {
		if !compactingSet[t.Path()] {
			newSSTables = append(newSSTables, t)
		}
	}
	if newTable != nil {
		newSSTables = append(newSSTables, newTable)
	}

	// Sort by path to restore oldest-to-newest order.
	// IMPORTANT: this relies on WriteToSStable producing filenames that sort
	// lexicographically in creation order (e.g. Unix-timestamp-prefixed names).
	// If the naming scheme ever changes, this sort must be updated accordingly.
	sort.Slice(newSSTables, func(i, j int) bool {
		return newSSTables[i].Path() < newSSTables[j].Path()
	})

	// Reopen all SSTables so their in-memory bloom filters and indexes
	// reflect the latest on-disk state.
	refreshed, err := refreshSSTables(newSSTables)
	if err != nil {
		return err
	}

	eng.sstables = refreshed
	return nil
}

func refreshSSTables(tables []sstable.SSTable) ([]sstable.SSTable, error) {
	refreshed := make([]sstable.SSTable, 0, len(tables))
	for _, t := range tables {
		reopened, err := sstable.OpenSStable(t.Path())
		if err != nil {
			return nil, fmt.Errorf("compaction: failed to refresh sstable %s: %w", t.Path(), err)
		}
		refreshed = append(refreshed, reopened)
	}
	return refreshed, nil
}

// backgroundCompactionLoop runs compaction on a fixed interval until ctx is cancelled.
// Start it in a goroutine: go eng.backgroundCompactionLoop(ctx)
func (eng *engine) backgroundCompactionLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <- eng.stopChan:
			return
		case <-ticker.C:
			// runCompaction acquires its own lock and handles the "nothing
			// to do" case internally — no need for a separate pre-check.
			if err := eng.runCompaction(); err != nil {
				fmt.Printf("background compaction error: %v\n", err)
			}
		}
	}
}