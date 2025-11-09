package hashkv

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestBasicDeleteOperation(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.db")

	db := NewHashKV(logPath)

	// Write a key-value pair
	if err := db.Write("key1", "value1"); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Verify it can be read
	value, err := db.Read("key1")
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if value != "value1" {
		t.Errorf("Expected 'value1', got '%s'", value)
	}

	// Delete the key
	if err := db.Delete("key1"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify read after delete returns error
	_, err = db.Read("key1")
	if err == nil {
		t.Error("Expected error when reading deleted key, got nil")
	}
}

func TestReadAfterDeleteReturnsError(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.db")

	db := NewHashKV(logPath)

	// Write and delete a key
	db.Write("testkey", "testvalue")
	db.Delete("testkey")

	// Attempt to read the deleted key
	value, err := db.Read("testkey")
	if err == nil {
		t.Error("Expected error when reading deleted key")
	}
	if value != "" {
		t.Errorf("Expected empty value for deleted key, got '%s'", value)
	}
}

func TestDeleteThenRewriteSameKey(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.db")

	db := NewHashKV(logPath)

	// Write, delete, then write again with different value
	db.Write("key1", "value1")
	db.Delete("key1")
	db.Write("key1", "value2")

	// Should read the new value
	value, err := db.Read("key1")
	if err != nil {
		t.Fatalf("Read failed after rewrite: %v", err)
	}
	if value != "value2" {
		t.Errorf("Expected 'value2' after rewrite, got '%s'", value)
	}
}

func TestBackwardCompatibilityWithOldFormat(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.db")

	// Manually create a file with old format entries
	file, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Write old format entry: {"key1":"value1"}
	oldEntry := []byte(`{"key1":"value1"}`)
	length := int64(len(oldEntry))
	binary.Write(file, binary.LittleEndian, length)
	binary.Write(file, binary.LittleEndian, oldEntry)

	// Write another old format entry
	oldEntry2 := []byte(`{"key2":"value2"}`)
	length2 := int64(len(oldEntry2))
	binary.Write(file, binary.LittleEndian, length2)
	binary.Write(file, binary.LittleEndian, oldEntry2)

	file.Close()

	// Open with HashKV and verify old entries can be read
	db := NewHashKV(logPath)

	value1, err := db.Read("key1")
	if err != nil {
		t.Fatalf("Failed to read old format key1: %v", err)
	}
	if value1 != "value1" {
		t.Errorf("Expected 'value1', got '%s'", value1)
	}

	value2, err := db.Read("key2")
	if err != nil {
		t.Fatalf("Failed to read old format key2: %v", err)
	}
	if value2 != "value2" {
		t.Errorf("Expected 'value2', got '%s'", value2)
	}

	// Write a new entry in new format
	if err := db.Write("key3", "value3"); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Verify new entry can be read
	value3, err := db.Read("key3")
	if err != nil {
		t.Fatalf("Failed to read new format key3: %v", err)
	}
	if value3 != "value3" {
		t.Errorf("Expected 'value3', got '%s'", value3)
	}
}

func TestIndexRebuildHandlesTombstones(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.db")

	// Create initial database and perform operations
	db := NewHashKV(logPath)
	db.Write("key1", "value1")
	db.Write("key2", "value2")
	db.Delete("key1") // Delete key1

	// Rebuild index by creating a new instance
	db2 := NewHashKV(logPath)

	// key1 should not be in index (was deleted)
	_, err := db2.Read("key1")
	if err == nil {
		t.Error("Expected error when reading deleted key after rebuild")
	}

	// key2 should still be readable
	value, err := db2.Read("key2")
	if err != nil {
		t.Fatalf("Failed to read key2 after rebuild: %v", err)
	}
	if value != "value2" {
		t.Errorf("Expected 'value2', got '%s'", value)
	}
}

func TestPersistenceAcrossRestarts(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.db")

	// First session: write and delete
	db1 := NewHashKV(logPath)
	db1.Write("persistent", "data")
	db1.Write("temporary", "data2")
	db1.Delete("temporary")

	// Second session: verify persistence
	db2 := NewHashKV(logPath)

	// persistent key should be readable
	value, err := db2.Read("persistent")
	if err != nil {
		t.Fatalf("Failed to read persistent key: %v", err)
	}
	if value != "data" {
		t.Errorf("Expected 'data', got '%s'", value)
	}

	// temporary key should not be readable
	_, err = db2.Read("temporary")
	if err == nil {
		t.Error("Expected error when reading deleted key after restart")
	}
}

func TestMultipleDeletesAndWrites(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.db")

	db := NewHashKV(logPath)

	// Write, delete, write, delete, write pattern
	db.Write("key", "v1")
	db.Delete("key")
	db.Write("key", "v2")
	db.Delete("key")
	db.Write("key", "v3")

	// Should read the latest value
	value, err := db.Read("key")
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if value != "v3" {
		t.Errorf("Expected 'v3', got '%s'", value)
	}

	// Verify persistence after rebuild
	db2 := NewHashKV(logPath)
	value2, err := db2.Read("key")
	if err != nil {
		t.Fatalf("Read after rebuild failed: %v", err)
	}
	if value2 != "v3" {
		t.Errorf("Expected 'v3' after rebuild, got '%s'", value2)
	}
}

func TestDeleteNonExistentKey(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.db")

	db := NewHashKV(logPath)

	// Delete a key that was never written
	if err := db.Delete("nonexistent"); err != nil {
		t.Fatalf("Delete of non-existent key should not error: %v", err)
	}

	// Verify it still can't be read
	_, err := db.Read("nonexistent")
	if err == nil {
		t.Error("Expected error when reading non-existent key")
	}
}

func TestTombstoneFormatInFile(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.db")

	db := NewHashKV(logPath)
	db.Write("key1", "value1")
	db.Delete("key1")

	// Read the file and verify tombstone format
	file, err := os.Open(logPath)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	// Skip first entry (the write)
	var length int64
	binary.Read(file, binary.LittleEndian, &length)
	file.Seek(length, 1)

	// Read second entry (the tombstone)
	binary.Read(file, binary.LittleEndian, &length)
	data := make([]byte, length)
	binary.Read(file, binary.LittleEndian, data)

	var entry HashKVEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		t.Fatalf("Failed to unmarshal tombstone: %v", err)
	}

	if entry.Key != "key1" {
		t.Errorf("Expected tombstone key 'key1', got '%s'", entry.Key)
	}
	if !entry.Deleted {
		t.Error("Expected Deleted flag to be true in tombstone")
	}
	if entry.Value != "" {
		t.Errorf("Expected empty value in tombstone, got '%s'", entry.Value)
	}
}
