package logdb

import (
	"os"
	"testing"
)

// TestCompactDuplicateKeys tests that compaction keeps only the latest value for duplicate keys.
// Currently this test FAILS because the compact function doesn't parse JSON - it just deduplicates
// exact duplicate lines. This means writing the same key with different values results in BOTH
// entries being kept instead of just the latest one.
func TestCompactDuplicateKeys(t *testing.T) {
	// Create a temporary test file
	tmpFile := "test_compact_duplicate.bin"
	defer os.Remove(tmpFile)

	// Create a new log and write the same key twice with different values
	log := NewLog(tmpFile)

	err := log.Write("test_key", "first_value")
	if err != nil {
		t.Fatalf("Failed to write first value: %v", err)
	}

	err = log.Write("test_key", "second_value")
	if err != nil {
		t.Fatalf("Failed to write second value: %v", err)
	}

	// Now create a new Log instance, which will trigger compaction
	log = NewLog(tmpFile)

	// Read the value - should only get the latest value
	value, err := log.Read("test_key")
	if err != nil {
		t.Fatalf("Failed to read value: %v", err)
	}

	// This should be "second_value" only, not "first_value"
	if value != "second_value" {
		t.Errorf("Expected 'second_value', got '%s'. The compact function should keep only the latest value for each key.", value)
	}

	// Additional check: verify the file only has one line (the compacted result)
	// If compact worked correctly, there should only be one entry for "test_key"
	file, err := os.Open(tmpFile)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	// Count lines in the file
	content, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	lines := 0
	for _, b := range content {
		if b == '\n' {
			lines++
		}
	}

	// After compaction, there should only be 1 line (one key with latest value)
	if lines != 1 {
		t.Errorf("Expected 1 line after compaction, got %d. The compact function should deduplicate by key, not by line.", lines)
	}
}
