package logdb

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Helper function to create a temporary log file with given content
func createTestLogFile(t *testing.T, content string) (string, error) {
	t.Helper()
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")

	file, err := os.Create(logPath)
	if err != nil {
		return "", fmt.Errorf("failed to create temp log file: %w", err)
	}
	defer file.Close()

	if content != "" {
		_, err = file.WriteString(content)
		if err != nil {
			return "", fmt.Errorf("failed to write content to temp log file: %w", err)
		}
	}
	return logPath, nil
}

// Helper function to read all lines from a file
func readFileContent(t *testing.T, filePath string) (string, error) {
	t.Helper()
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open file for reading: %w", err)
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("failed to scan file: %w", err)
	}
	return strings.Join(lines, "\n"), nil
}

func TestCompact(t *testing.T) {
	tests := []struct {
		name           string
		initialContent string
		expectedEntries map[string]string
	}{
		{
			name:           "EmptyFile",
			initialContent: "",
			expectedEntries: map[string]string{},
		},
		{
			name: "DuplicateKeys",
			initialContent: `{"key1":"value1"}
{"key2":"value2"}
{"key1":"value3"}
{"key3":"value4"}
{"key2":"value5"}`,
			expectedEntries: map[string]string{
				"key1": "value3",
				"key2": "value5",
				"key3": "value4",
			},
		},
		{
			name: "MalformedLines",
			initialContent: `{"key1":"value1"}
{"key2":"value2"}
malformed_line
{"key1":"value3"}
{"key3":"value4"}`,
			expectedEntries: map[string]string{
				"key1": "value3",
				"key2": "value2",
				"key3": "value4",
			},
		},
		{
			name: "NoDuplicates",
			initialContent: `{"keyA":"valueA"}
{"keyB":"valueB"}`,
			expectedEntries: map[string]string{
				"keyA": "valueA",
				"keyB": "valueB",
			},
		},
		{
			name: "SingleEntry",
			initialContent: `{"keyS":"valueS"}`,
			expectedEntries: map[string]string{
				"keyS": "valueS",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logPath, err := createTestLogFile(t, tt.initialContent)
			if err != nil {
				t.Fatalf("Failed to create test log file: %v", err)
			}

			logDB := &Log{LogPath: logPath}
			if err := compact(logDB); err != nil {
				t.Fatalf("compact failed for test case %s: %v", tt.name, err)
			}

			content, err := readFileContent(t, logPath)
			if err != nil {
				t.Fatalf("Failed to read compacted file for test case %s: %v", tt.name, err)
			}

			lines := strings.Split(content, "\n")
			if len(lines) > 0 && lines[len(lines)-1] == "" { // Remove trailing newline if present
				lines = lines[:len(lines)-1]
			}

			if len(lines) != len(tt.expectedEntries) {
				t.Fatalf("Expected %d entries after compaction, but got %d. Content:\n%s", len(tt.expectedEntries), len(lines), content)
			}

			actualEntries := make(map[string]string)
			for _, line := range lines {
				// Try new format first
				var newEntry LogEntry
				if err := json.Unmarshal([]byte(line), &newEntry); err == nil && newEntry.Key != "" {
					actualEntries[newEntry.Key] = newEntry.Value
				} else {
					// Fall back to old format
					var entry map[string]string
					if err := json.Unmarshal([]byte(line), &entry); err != nil {
						t.Errorf("Failed to unmarshal line '%s' from compacted file: %v", line, err)
						continue
					}
					for k, v := range entry {
						actualEntries[k] = v
					}
				}
			}

			if len(actualEntries) != len(tt.expectedEntries) {
				t.Errorf("Mismatched number of entries. Expected %d, Got %d. Actual: %v, Expected: %v", len(tt.expectedEntries), len(actualEntries), actualEntries, tt.expectedEntries)
			}

			for k, expectedV := range tt.expectedEntries {
				if actualV, ok := actualEntries[k]; !ok || actualV != expectedV {
					t.Errorf("Mismatched entry for key '%s'. Expected '%s', Got '%s'", k, expectedV, actualV)
				}
			}
		})
	}
}

// Test NewLog to ensure it handles file creation and compaction correctly
func TestNewLog(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")

	// Test case 1: New log file creation
	logDB1, err := NewLog(logPath) // NewLog now returns an error
	if err != nil {
		t.Fatalf("NewLog returned an error for a new file: %v", err)
	}
	if logDB1 == nil {
		t.Fatal("NewLog returned nil for a new file")
	}
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		t.Errorf("NewLog did not create the log file: %v", err)
	}

	// Test case 2: Compaction on existing file
	// Write some initial data
	initialContent := `{"key1":"value1"}
{"key2":"value2"}
{"key1":"value3"}`
	if err := os.WriteFile(logPath, []byte(initialContent), 0644); err != nil {
		t.Fatalf("Failed to write initial content for compaction test: %v", err)
	}

	// Re-initialize Log to trigger compaction
	logDB2, err := NewLog(logPath) // NewLog now returns an error
	if err != nil {
		t.Fatalf("NewLog returned an error for an existing file requiring compaction: %v", err)
	}
	if logDB2 == nil {
		t.Fatal("NewLog returned nil for an existing file requiring compaction")
	}

	// Read content to verify compaction
	content, err := readFileContent(t, logPath)
	if err != nil {
		t.Fatalf("Failed to read compacted file after NewLog: %v", err)
	}

	expectedEntries := map[string]string{
		"key1": "value3",
		"key2": "value2",
	}
	lines := strings.Split(content, "\n")
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}

	if len(lines) != len(expectedEntries) {
		t.Fatalf("Expected %d entries after NewLog compaction, but got %d. Content:\n%s", len(expectedEntries), len(lines), content)
	}

	for _, line := range lines {
		// Try new format first
		var newEntry LogEntry
		if err := json.Unmarshal([]byte(line), &newEntry); err == nil && newEntry.Key != "" {
			if expectedV, ok := expectedEntries[newEntry.Key]; !ok || newEntry.Value != expectedV {
				t.Errorf("Unexpected entry in compacted file after NewLog: %s:%s. Expected %s:%s", newEntry.Key, newEntry.Value, newEntry.Key, expectedV)
			}
		} else {
			// Fall back to old format
			var entry map[string]string
			if err := json.Unmarshal([]byte(line), &entry); err != nil {
				t.Errorf("Failed to unmarshal line '%s' from compacted file after NewLog: %v", line, err)
				continue
			}
			for k, v := range entry {
				if expectedV, ok := expectedEntries[k]; !ok || v != expectedV {
					t.Errorf("Unexpected entry in compacted file after NewLog: %s:%s. Expected %s:%s", k, v, k, expectedV)
				}
			}
		}
	}
}

// Test delete operations
func TestDelete(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")

	// Create a new log
	logDB, err := NewLog(logPath)
	if err != nil {
		t.Fatalf("Failed to create new log: %v", err)
	}

	// Write a key-value pair
	if err := logDB.Write("key1", "value1"); err != nil {
		t.Fatalf("Failed to write key1: %v", err)
	}

	// Verify we can read it
	value, err := logDB.Read("key1")
	if err != nil {
		t.Fatalf("Failed to read key1: %v", err)
	}
	if value != "value1" {
		t.Errorf("Expected value1, got %s", value)
	}

	// Delete the key
	if err := logDB.Delete("key1"); err != nil {
		t.Fatalf("Failed to delete key1: %v", err)
	}

	// Try to read the deleted key - should return error
	_, err = logDB.Read("key1")
	if err == nil {
		t.Error("Expected error when reading deleted key, got nil")
	}
	if !strings.Contains(err.Error(), "deleted") {
		t.Errorf("Expected error message to contain 'deleted', got: %v", err)
	}
}

// Test compaction removes tombstones
func TestCompactRemovesTombstones(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")

	// Create a log with some data and deletions
	initialContent := `{"key":"key1","value":"value1","deleted":false}
{"key":"key2","value":"value2","deleted":false}
{"key":"key1","value":"","deleted":true}
{"key":"key3","value":"value3","deleted":false}`

	if err := os.WriteFile(logPath, []byte(initialContent), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	// Compact the log
	logDB := &Log{LogPath: logPath}
	if err := compact(logDB); err != nil {
		t.Fatalf("Failed to compact: %v", err)
	}

	// Read and verify compacted content
	content, err := readFileContent(t, logPath)
	if err != nil {
		t.Fatalf("Failed to read compacted file: %v", err)
	}

	// Parse entries
	lines := strings.Split(content, "\n")
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}

	actualEntries := make(map[string]LogEntry)
	for _, line := range lines {
		var entry LogEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("Failed to unmarshal line: %v", err)
		}
		actualEntries[entry.Key] = entry
	}

	// key1 should be gone (was deleted)
	if _, exists := actualEntries["key1"]; exists {
		t.Error("key1 should have been removed during compaction")
	}

	// key2 and key3 should still exist
	if entry, exists := actualEntries["key2"]; !exists || entry.Value != "value2" {
		t.Errorf("key2 should exist with value2, got: %v", entry)
	}
	if entry, exists := actualEntries["key3"]; !exists || entry.Value != "value3" {
		t.Errorf("key3 should exist with value3, got: %v", entry)
	}
}

// Test backward compatibility with old format
func TestBackwardCompatibility(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")

	// Create a log file with old format entries
	oldFormatContent := `{"key1":"value1"}
{"key2":"value2"}
{"key3":"value3"}
`

	if err := os.WriteFile(logPath, []byte(oldFormatContent), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	logDB := &Log{LogPath: logPath}

	// Should be able to read old format
	value, err := logDB.Read("key1")
	if err != nil {
		t.Fatalf("Failed to read key1 from old format: %v", err)
	}
	if value != "value1" {
		t.Errorf("Expected value1, got %s", value)
	}

	// Write a new entry (new format)
	if err := logDB.Write("key4", "value4"); err != nil {
		t.Fatalf("Failed to write key4: %v", err)
	}

	// Should be able to read both old and new format
	value, err = logDB.Read("key4")
	if err != nil {
		t.Fatalf("Failed to read key4: %v", err)
	}
	if value != "value4" {
		t.Errorf("Expected value4, got %s", value)
	}

	// Compact should convert old format to new format
	if err := compact(logDB); err != nil {
		t.Fatalf("Failed to compact: %v", err)
	}

	// All keys should still be readable
	expectedValues := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
	}

	for key, expectedValue := range expectedValues {
		value, err := logDB.Read(key)
		if err != nil {
			t.Errorf("Failed to read %s after compaction: %v", key, err)
		}
		if value != expectedValue {
			t.Errorf("Expected %s for key %s, got %s", expectedValue, key, value)
		}
	}
}

// Test Read returns error for non-existent key
func TestReadNonExistentKey(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")

	logDB, err := NewLog(logPath)
	if err != nil {
		t.Fatalf("Failed to create new log: %v", err)
	}

	// Write some data
	if err := logDB.Write("key1", "value1"); err != nil {
		t.Fatalf("Failed to write key1: %v", err)
	}

	// Try to read a non-existent key
	_, err = logDB.Read("nonexistent")
	if err == nil {
		t.Error("Expected error when reading non-existent key, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("Expected error message to contain 'not found', got: %v", err)
	}
}

// Test delete and re-write
func TestDeleteAndRewrite(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")

	logDB, err := NewLog(logPath)
	if err != nil {
		t.Fatalf("Failed to create new log: %v", err)
	}

	// Write a key
	if err := logDB.Write("key1", "value1"); err != nil {
		t.Fatalf("Failed to write key1: %v", err)
	}

	// Delete the key
	if err := logDB.Delete("key1"); err != nil {
		t.Fatalf("Failed to delete key1: %v", err)
	}

	// Write the same key again with a new value
	if err := logDB.Write("key1", "value2"); err != nil {
		t.Fatalf("Failed to write key1 again: %v", err)
	}

	// Should be able to read the new value
	value, err := logDB.Read("key1")
	if err != nil {
		t.Fatalf("Failed to read key1 after rewrite: %v", err)
	}
	if value != "value2" {
		t.Errorf("Expected value2, got %s", value)
	}

	// After compaction, should still have the latest value
	if err := compact(logDB); err != nil {
		t.Fatalf("Failed to compact: %v", err)
	}

	value, err = logDB.Read("key1")
	if err != nil {
		t.Fatalf("Failed to read key1 after compaction: %v", err)
	}
	if value != "value2" {
		t.Errorf("Expected value2 after compaction, got %s", value)
	}
}
