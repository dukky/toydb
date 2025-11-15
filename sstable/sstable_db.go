package sstable

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	// MemtableFlushThreshold is the size in bytes after which memtable is flushed
	MemtableFlushThreshold = 1024 * 1024 // 1MB
	// CompactionThreshold is the number of SSTables that trigger compaction
	CompactionThreshold = 4
)

// SSTableDB implements a database using SSTables with a memtable
type SSTableDB struct {
	mu          sync.RWMutex
	dataDir     string
	memtable    *Memtable
	sstables    []*SSTable // Ordered from newest to oldest
	nextSSTableID int
}

// NewSSTableDB creates a new SSTable-based database
func NewSSTableDB(dataDir string) (*SSTableDB, error) {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	db := &SSTableDB{
		dataDir:     dataDir,
		memtable:    NewMemtable(),
		sstables:    make([]*SSTable, 0),
		nextSSTableID: 0,
	}

	// Load existing SSTables
	if err := db.loadSSTables(); err != nil {
		return nil, err
	}

	return db, nil
}

// loadSSTables loads all existing SSTable files from the data directory
func (db *SSTableDB) loadSSTables() error {
	files, err := os.ReadDir(db.dataDir)
	if err != nil {
		return fmt.Errorf("failed to read data directory: %w", err)
	}

	var sstablePaths []string
	maxID := -1

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		// Look for files matching pattern: sstable_XXXXXX.sst
		if strings.HasPrefix(file.Name(), "sstable_") && strings.HasSuffix(file.Name(), ".sst") {
			sstablePaths = append(sstablePaths, filepath.Join(db.dataDir, file.Name()))

			// Extract ID to determine next ID
			idStr := strings.TrimPrefix(file.Name(), "sstable_")
			idStr = strings.TrimSuffix(idStr, ".sst")
			if id, err := strconv.Atoi(idStr); err == nil && id > maxID {
				maxID = id
			}
		}
	}

	// Sort by ID (newest first)
	sort.Slice(sstablePaths, func(i, j int) bool {
		return sstablePaths[i] > sstablePaths[j]
	})

	// Load SSTables
	for _, path := range sstablePaths {
		sst, err := OpenSSTable(path)
		if err != nil {
			return fmt.Errorf("failed to open SSTable %s: %w", path, err)
		}
		db.sstables = append(db.sstables, sst)
	}

	db.nextSSTableID = maxID + 1

	return nil
}

// Write writes a key-value pair to the database
func (db *SSTableDB) Write(key, value string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Write to memtable
	db.memtable.Put(key, value)

	// Flush memtable if it exceeds threshold
	if db.memtable.Size() >= MemtableFlushThreshold {
		if err := db.flushMemtable(); err != nil {
			return fmt.Errorf("failed to flush memtable: %w", err)
		}

		// Check if compaction is needed
		if len(db.sstables) >= CompactionThreshold {
			if err := db.compact(); err != nil {
				return fmt.Errorf("failed to compact: %w", err)
			}
		}
	}

	return nil
}

// Read reads a value by key from the database
func (db *SSTableDB) Read(key string) (string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Check memtable first
	if value, found := db.memtable.Get(key); found {
		return value, nil
	}

	// Check SSTables from newest to oldest
	for _, sst := range db.sstables {
		value, found, err := sst.Get(key)
		if err != nil {
			return "", fmt.Errorf("failed to read from SSTable: %w", err)
		}
		if found {
			// If found but value is empty, it's a tombstone
			if value == "" {
				return "", fmt.Errorf("key not found: %s", key)
			}
			return value, nil
		}
	}

	return "", fmt.Errorf("key not found: %s", key)
}

// Delete marks a key as deleted in the database
func (db *SSTableDB) Delete(key string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Write tombstone to memtable
	db.memtable.Delete(key)

	// Flush memtable if it exceeds threshold
	if db.memtable.Size() >= MemtableFlushThreshold {
		if err := db.flushMemtable(); err != nil {
			return fmt.Errorf("failed to flush memtable: %w", err)
		}

		// Check if compaction is needed
		if len(db.sstables) >= CompactionThreshold {
			if err := db.compact(); err != nil {
				return fmt.Errorf("failed to compact: %w", err)
			}
		}
	}

	return nil
}

// flushMemtable writes the current memtable to disk as an SSTable
func (db *SSTableDB) flushMemtable() error {
	if db.memtable.IsEmpty() {
		return nil
	}

	// Get sorted entries from memtable
	entries := db.memtable.GetSortedEntries()

	// Generate SSTable filename
	sstablePath := filepath.Join(db.dataDir, fmt.Sprintf("sstable_%06d.sst", db.nextSSTableID))
	db.nextSSTableID++

	// Write SSTable to disk
	if err := WriteSSTable(sstablePath, entries); err != nil {
		return err
	}

	// Open the newly created SSTable
	sst, err := OpenSSTable(sstablePath)
	if err != nil {
		return err
	}

	// Add to SSTables list (newest first)
	db.sstables = append([]*SSTable{sst}, db.sstables...)

	// Clear memtable
	db.memtable.Clear()

	return nil
}

// compact merges all SSTables into a single SSTable
func (db *SSTableDB) compact() error {
	if len(db.sstables) < 2 {
		return nil
	}

	// Collect all entries from all SSTables
	allEntries := make(map[string]Entry)

	// Read from oldest to newest (reversed order)
	for i := len(db.sstables) - 1; i >= 0; i-- {
		entries, err := db.sstables[i].GetAllEntries()
		if err != nil {
			return fmt.Errorf("failed to read SSTable entries: %w", err)
		}

		// Latest value for each key wins
		for _, entry := range entries {
			allEntries[entry.Key] = entry
		}
	}

	// Convert map to sorted slice, excluding deleted entries
	var compactedEntries []Entry
	for _, entry := range allEntries {
		if !entry.Deleted {
			compactedEntries = append(compactedEntries, entry)
		}
	}

	if len(compactedEntries) == 0 {
		// All entries were deleted, just remove all SSTables
		for _, sst := range db.sstables {
			os.Remove(sst.FilePath)
		}
		db.sstables = nil
		return nil
	}

	// Sort entries by key
	sort.Slice(compactedEntries, func(i, j int) bool {
		return compactedEntries[i].Key < compactedEntries[j].Key
	})

	// Create new compacted SSTable with timestamp to avoid conflicts
	compactedPath := filepath.Join(db.dataDir, fmt.Sprintf("sstable_%06d.sst", db.nextSSTableID))
	db.nextSSTableID++

	if err := WriteSSTable(compactedPath, compactedEntries); err != nil {
		return err
	}

	// Open the compacted SSTable
	compactedSST, err := OpenSSTable(compactedPath)
	if err != nil {
		return err
	}

	// Remove old SSTable files
	for _, sst := range db.sstables {
		os.Remove(sst.FilePath)
	}

	// Replace with compacted SSTable
	db.sstables = []*SSTable{compactedSST}

	return nil
}

// Compact manually triggers compaction of all SSTables
func (db *SSTableDB) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Flush memtable first
	if !db.memtable.IsEmpty() {
		if err := db.flushMemtable(); err != nil {
			return err
		}
	}

	return db.compact()
}

// Flush manually flushes the memtable to disk
func (db *SSTableDB) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.flushMemtable()
}

// Stats returns statistics about the database
func (db *SSTableDB) Stats() map[string]interface{} {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return map[string]interface{}{
		"memtable_size":   db.memtable.Size(),
		"num_sstables":    len(db.sstables),
		"next_sstable_id": db.nextSSTableID,
	}
}

// Close closes the database and flushes any remaining data
func (db *SSTableDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Flush memtable before closing
	if !db.memtable.IsEmpty() {
		return db.flushMemtable()
	}

	return nil
}
