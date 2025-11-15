package sstable

import (
	"path/filepath"
	"testing"
)

func TestMemtable(t *testing.T) {
	mem := NewMemtable()

	// Test Put and Get
	mem.Put("key1", "value1")
	mem.Put("key2", "value2")

	val, found := mem.Get("key1")
	if !found || val != "value1" {
		t.Errorf("Expected value1, got %s", val)
	}

	val, found = mem.Get("key2")
	if !found || val != "value2" {
		t.Errorf("Expected value2, got %s", val)
	}

	// Test overwrite
	mem.Put("key1", "new_value1")
	val, found = mem.Get("key1")
	if !found || val != "new_value1" {
		t.Errorf("Expected new_value1, got %s", val)
	}

	// Test not found
	_, found = mem.Get("nonexistent")
	if found {
		t.Error("Expected key not to be found")
	}
}

func TestMemtableDelete(t *testing.T) {
	mem := NewMemtable()

	mem.Put("key1", "value1")
	mem.Delete("key1")

	_, found := mem.Get("key1")
	if found {
		t.Error("Expected key to be deleted")
	}
}

func TestMemtableSortedEntries(t *testing.T) {
	mem := NewMemtable()

	// Insert in random order
	mem.Put("zebra", "z")
	mem.Put("apple", "a")
	mem.Put("mango", "m")
	mem.Put("banana", "b")

	entries := mem.GetSortedEntries()

	// Verify sorted order
	expectedKeys := []string{"apple", "banana", "mango", "zebra"}
	if len(entries) != len(expectedKeys) {
		t.Fatalf("Expected %d entries, got %d", len(expectedKeys), len(entries))
	}

	for i, entry := range entries {
		if entry.Key != expectedKeys[i] {
			t.Errorf("Expected key %s at position %d, got %s", expectedKeys[i], i, entry.Key)
		}
	}
}

func TestSSTableWriteAndRead(t *testing.T) {
	tempDir := t.TempDir()
	sstablePath := filepath.Join(tempDir, "test.sst")

	// Create test entries
	entries := []Entry{
		{Key: "apple", Value: "red"},
		{Key: "banana", Value: "yellow"},
		{Key: "cherry", Value: "red"},
		{Key: "date", Value: "brown"},
	}

	// Write SSTable
	if err := WriteSSTable(sstablePath, entries); err != nil {
		t.Fatalf("Failed to write SSTable: %v", err)
	}

	// Open SSTable
	sst, err := OpenSSTable(sstablePath)
	if err != nil {
		t.Fatalf("Failed to open SSTable: %v", err)
	}

	// Test reads
	val, found, err := sst.Get("banana")
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}
	if !found || val != "yellow" {
		t.Errorf("Expected yellow, got %s", val)
	}

	val, found, err = sst.Get("apple")
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}
	if !found || val != "red" {
		t.Errorf("Expected red, got %s", val)
	}

	// Test non-existent key
	_, found, err = sst.Get("grape")
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}
	if found {
		t.Error("Expected key not to be found")
	}
}

func TestSSTableWithTombstones(t *testing.T) {
	tempDir := t.TempDir()
	sstablePath := filepath.Join(tempDir, "test.sst")

	// Create test entries with tombstone
	entries := []Entry{
		{Key: "apple", Value: "red"},
		{Key: "banana", Value: "", Deleted: true},
		{Key: "cherry", Value: "red"},
	}

	// Write SSTable
	if err := WriteSSTable(sstablePath, entries); err != nil {
		t.Fatalf("Failed to write SSTable: %v", err)
	}

	// Open SSTable
	sst, err := OpenSSTable(sstablePath)
	if err != nil {
		t.Fatalf("Failed to open SSTable: %v", err)
	}

	// Test deleted key - should return found=true with empty value (tombstone)
	val, found, err := sst.Get("banana")
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}
	if !found {
		t.Error("Expected tombstone to be found")
	}
	if val != "" {
		t.Errorf("Expected empty value for tombstone, got %s", val)
	}

	// Test other keys still work
	val, found, err = sst.Get("apple")
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}
	if !found || val != "red" {
		t.Errorf("Expected red, got %s", val)
	}
}

func TestSSTableDB(t *testing.T) {
	tempDir := t.TempDir()

	db, err := NewSSTableDB(tempDir)
	if err != nil {
		t.Fatalf("Failed to create SSTableDB: %v", err)
	}
	defer db.Close()

	// Test write and read
	if err := db.Write("key1", "value1"); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	val, err := db.Read("key1")
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}
	if val != "value1" {
		t.Errorf("Expected value1, got %s", val)
	}

	// Test overwrite
	if err := db.Write("key1", "new_value1"); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	val, err = db.Read("key1")
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}
	if val != "new_value1" {
		t.Errorf("Expected new_value1, got %s", val)
	}

	// Test delete
	if err := db.Delete("key1"); err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	_, err = db.Read("key1")
	if err == nil {
		t.Error("Expected error reading deleted key")
	}
}

func TestSSTableDBFlush(t *testing.T) {
	tempDir := t.TempDir()

	db, err := NewSSTableDB(tempDir)
	if err != nil {
		t.Fatalf("Failed to create SSTableDB: %v", err)
	}

	// Write some data
	if err := db.Write("key1", "value1"); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	// Manually flush
	if err := db.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Close and reopen
	db.Close()

	db2, err := NewSSTableDB(tempDir)
	if err != nil {
		t.Fatalf("Failed to reopen SSTableDB: %v", err)
	}
	defer db2.Close()

	// Verify data persisted
	val, err := db2.Read("key1")
	if err != nil {
		t.Fatalf("Failed to read after reopen: %v", err)
	}
	if val != "value1" {
		t.Errorf("Expected value1, got %s", val)
	}
}

func TestSSTableDBCompaction(t *testing.T) {
	tempDir := t.TempDir()

	db, err := NewSSTableDB(tempDir)
	if err != nil {
		t.Fatalf("Failed to create SSTableDB: %v", err)
	}
	defer db.Close()

	// Write data and flush multiple times to create multiple SSTables
	for i := 0; i < 5; i++ {
		if err := db.Write("key1", "value1"); err != nil {
			t.Fatalf("Failed to write: %v", err)
		}
		if err := db.Flush(); err != nil {
			t.Fatalf("Failed to flush: %v", err)
		}
	}

	// Trigger compaction
	if err := db.Compact(); err != nil {
		t.Fatalf("Failed to compact: %v", err)
	}

	stats := db.Stats()
	numSSTables := stats["num_sstables"].(int)

	// After compaction, should have fewer SSTables
	if numSSTables > 1 {
		t.Errorf("Expected 1 SSTable after compaction, got %d", numSSTables)
	}

	// Verify data is still accessible
	val, err := db.Read("key1")
	if err != nil {
		t.Fatalf("Failed to read after compaction: %v", err)
	}
	if val != "value1" {
		t.Errorf("Expected value1, got %s", val)
	}
}

func TestSSTableDBCompactionWithDeletes(t *testing.T) {
	tempDir := t.TempDir()

	db, err := NewSSTableDB(tempDir)
	if err != nil {
		t.Fatalf("Failed to create SSTableDB: %v", err)
	}
	defer db.Close()

	// Write and delete keys
	if err := db.Write("key1", "value1"); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	if err := db.Delete("key1"); err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Compact to remove tombstones
	if err := db.Compact(); err != nil {
		t.Fatalf("Failed to compact: %v", err)
	}

	// Verify key is still deleted after compaction
	_, err = db.Read("key1")
	if err == nil {
		t.Error("Expected error reading deleted key after compaction")
	}
}

func TestSSTableDBLargeDataset(t *testing.T) {
	tempDir := t.TempDir()

	db, err := NewSSTableDB(tempDir)
	if err != nil {
		t.Fatalf("Failed to create SSTableDB: %v", err)
	}
	defer db.Close()

	// Write many keys to trigger automatic flush and compaction
	numKeys := 1000
	for i := 0; i < numKeys; i++ {
		key := "key" + string(rune(i))
		value := "value" + string(rune(i))
		if err := db.Write(key, value); err != nil {
			t.Fatalf("Failed to write key %s: %v", key, err)
		}
	}

	// Verify all keys are readable
	for i := 0; i < numKeys; i++ {
		key := "key" + string(rune(i))
		expectedValue := "value" + string(rune(i))
		val, err := db.Read(key)
		if err != nil {
			t.Fatalf("Failed to read key %s: %v", key, err)
		}
		if val != expectedValue {
			t.Errorf("For key %s, expected %s, got %s", key, expectedValue, val)
		}
	}
}

func TestSSTableDBPersistence(t *testing.T) {
	tempDir := t.TempDir()

	// Create DB and write data
	db, err := NewSSTableDB(tempDir)
	if err != nil {
		t.Fatalf("Failed to create SSTableDB: %v", err)
	}

	for i := 0; i < 100; i++ {
		key := "key" + string(rune(i))
		value := "value" + string(rune(i))
		if err := db.Write(key, value); err != nil {
			t.Fatalf("Failed to write: %v", err)
		}
	}

	db.Close()

	// Reopen and verify data
	db2, err := NewSSTableDB(tempDir)
	if err != nil {
		t.Fatalf("Failed to reopen SSTableDB: %v", err)
	}
	defer db2.Close()

	// Verify data persisted
	for i := 0; i < 100; i++ {
		key := "key" + string(rune(i))
		expectedValue := "value" + string(rune(i))
		val, err := db2.Read(key)
		if err != nil {
			t.Fatalf("Failed to read key %s after reopen: %v", key, err)
		}
		if val != expectedValue {
			t.Errorf("For key %s, expected %s, got %s", key, expectedValue, val)
		}
	}
}
