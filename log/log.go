package logdb

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"syscall"
)

// CompactionThreshold is the minimum file size (in bytes) that triggers
// automatic compaction when opening a log database. Files smaller than
// this threshold will not be compacted automatically to improve performance.
// Default: 1MB (1024 * 1024 bytes)
const CompactionThreshold = 1024 * 1024

type Log struct {
	LogPath  string
	mu       sync.RWMutex // Protects concurrent access within the same process
	lockFile *os.File     // File-based lock to prevent multi-process access
}

// LogEntry represents a single entry in the log file
type LogEntry struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Deleted bool   `json:"deleted"`
}

func (l *Log) Write(key string, value string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	file, err := os.OpenFile(l.LogPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	entry := LogEntry{
		Key:     key,
		Value:   value,
		Deleted: false,
	}
	marshalled, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("error marshalling json: %v", err)
	}
	_, err = file.Write(marshalled)
	if err != nil {
		return fmt.Errorf("error writing data: %v", err)
	}
	_, err = file.Write([]byte("\n"))
	if err != nil {
		return fmt.Errorf("error writing newline: %v", err)
	}

	// Ensure data is written to disk
	if err := file.Sync(); err != nil {
		return fmt.Errorf("error syncing file: %v", err)
	}

	return nil
}

// Delete marks a key as deleted by writing a tombstone entry
func (l *Log) Delete(key string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	file, err := os.OpenFile(l.LogPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	entry := LogEntry{
		Key:     key,
		Value:   "",
		Deleted: true,
	}
	marshalled, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("error marshalling json: %v", err)
	}
	_, err = file.Write(marshalled)
	if err != nil {
		return fmt.Errorf("error writing data: %v", err)
	}
	_, err = file.Write([]byte("\n"))
	if err != nil {
		return fmt.Errorf("error writing newline: %v", err)
	}

	// Ensure data is written to disk
	if err := file.Sync(); err != nil {
		return fmt.Errorf("error syncing file: %v", err)
	}

	return nil
}

func (l *Log) Read(key string) (string, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	file, err := os.Open(l.LogPath)
	if err != nil {
		return "", fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var latestValue string
	var found bool
	var isDeleted bool

	for scanner.Scan() {
		line := scanner.Bytes()

		// Try to unmarshal as new format first
		var entry LogEntry
		if err := json.Unmarshal(line, &entry); err == nil && entry.Key != "" {
			// New format: {"key":"k","value":"v","deleted":false}
			if entry.Key == key {
				found = true
				latestValue = entry.Value
				isDeleted = entry.Deleted
			}
		} else {
			// Try old format: {"key":"value"}
			var oldEntry map[string]string
			if err := json.Unmarshal(line, &oldEntry); err == nil {
				if value, exists := oldEntry[key]; exists {
					found = true
					latestValue = value
					isDeleted = false // Old format doesn't have deletes
				}
			}
			// Ignore malformed lines
		}
	}

	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("error scanning file: %v", err)
	}

	if !found {
		return "", fmt.Errorf("key not found: %s", key)
	}

	if isDeleted {
		return "", fmt.Errorf("key has been deleted: %s", key)
	}

	return latestValue, nil
}

func NewLog(logPath string) (*Log, error) {
	db := &Log{
		LogPath: logPath,
	}

	// Acquire file-based lock to prevent multi-process access
	lockPath := logPath + ".lock"
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("error creating lock file: %w", err)
	}

	// Try to acquire exclusive lock (LOCK_EX | LOCK_NB = non-blocking exclusive lock)
	err = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		lockFile.Close()
		return nil, fmt.Errorf("database is already in use by another process: %w", err)
	}

	db.lockFile = lockFile

	fileInfo, err := os.Stat(logPath)
	if os.IsNotExist(err) {
		file, err := os.Create(logPath)
		if err != nil {
			// Release lock before returning error
			db.Close()
			return nil, fmt.Errorf("error creating log file: %w", err)
		}
		file.Close() // Close immediately as we don't need the handle here
		return db, nil
	}
	// If file exists and exceeds threshold, compact it
	if fileInfo.Size() >= CompactionThreshold {
		// Protect auto-compaction with mutex
		db.mu.Lock()
		err = compact(db)
		db.mu.Unlock()
		if err != nil {
			// Release lock before returning error
			db.Close()
			return nil, fmt.Errorf("error during log compaction: %w", err)
		}
	}
	return db, nil
}

// Close releases the file lock and closes the database.
// This must be called when done using the database to allow other processes to access it.
func (l *Log) Close() error {
	if l.lockFile == nil {
		return nil
	}

	// Release the file lock
	if err := syscall.Flock(int(l.lockFile.Fd()), syscall.LOCK_UN); err != nil {
		return fmt.Errorf("error releasing lock: %w", err)
	}

	// Close the lock file
	if err := l.lockFile.Close(); err != nil {
		return fmt.Errorf("error closing lock file: %w", err)
	}

	// Remove the lock file
	lockPath := l.LogPath + ".lock"
	if err := os.Remove(lockPath); err != nil {
		// Don't return error if file doesn't exist
		if !os.IsNotExist(err) {
			return fmt.Errorf("error removing lock file: %w", err)
		}
	}

	l.lockFile = nil
	return nil
}

// Compact manually triggers compaction of the log file, removing duplicate
// entries and tombstones regardless of file size. This can be used to reclaim
// space or optimize read performance on demand.
func (l *Log) Compact() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return compact(l)
}

func compact(l *Log) error {
	// Open the original log file for reading
	readFile, err := os.Open(l.LogPath)
	if err != nil {
		return fmt.Errorf("error opening file for reading: %v", err)
	}
	defer readFile.Close()

	scanner := bufio.NewScanner(readFile)
	latestEntries := make(map[string]LogEntry)

	// Read all entries and keep the latest entry for each key
	for scanner.Scan() {
		line := scanner.Bytes()

		// Try to unmarshal as new format first
		var entry LogEntry
		if err := json.Unmarshal(line, &entry); err == nil && entry.Key != "" {
			// New format: {"key":"k","value":"v","deleted":false}
			latestEntries[entry.Key] = entry
		} else {
			// Try old format: {"key":"value"}
			var oldEntry map[string]string
			if err := json.Unmarshal(line, &oldEntry); err == nil {
				for k, v := range oldEntry {
					latestEntries[k] = LogEntry{
						Key:     k,
						Value:   v,
						Deleted: false,
					}
				}
			}
			// Ignore malformed lines
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error scanning file: %v", err)
	}

	// Create a temporary file for writing compacted data
	// Use a pattern that ensures a unique filename and is in the same directory
	// to ensure atomic rename across different filesystems is possible.
	tmpFile, err := os.CreateTemp(l.LogPath[:strings.LastIndex(l.LogPath, "/")+1], "compact-*.log")
	if err != nil {
		return fmt.Errorf("error creating temporary file: %v", err)
	}
	tmpPath := tmpFile.Name()
	defer func() {
		// Clean up the temporary file if an error occurs before rename
		if err != nil {
			os.Remove(tmpPath)
		}
	}()
	defer tmpFile.Close()

	// Write the compacted data to the temporary file
	// Skip entries that are marked as deleted (tombstones)
	for _, entry := range latestEntries {
		if entry.Deleted {
			// Skip tombstone entries - they are removed during compaction
			continue
		}

		marshalled, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("error marshalling json: %v", err)
		}
		_, err = tmpFile.Write(marshalled)
		if err != nil {
			return fmt.Errorf("error writing data to temp file: %v", err)
		}
		_, err = tmpFile.Write([]byte("\n"))
		if err != nil {
			return fmt.Errorf("error writing newline to temp file: %v", err)
		}
	}

	// Atomically replace the original log file with the temporary file
	if err := os.Rename(tmpPath, l.LogPath); err != nil {
		return fmt.Errorf("error renaming temporary file to %s: %v", l.LogPath, err)
	}

	return nil
}
