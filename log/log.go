package logdb

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type Log struct {
	LogPath string
}

func (l *Log) Write(key string, value string) error {
	file, err := os.OpenFile(l.LogPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	marshalled, err := json.Marshal(map[string]string{
		key: value,
	})
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

	return nil
}

func (l *Log) Read(key string) (string, error) {
	file, err := os.Open(l.LogPath)
	if err != nil {
		return "", fmt.Errorf("error opening file: %v", err)
	}
	scanner := bufio.NewScanner(file)
	latest := ""
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "{\""+key+"\":") {
			latest = strings.TrimSuffix(strings.TrimPrefix(line, "{\""+key+"\":\""), "\"}")
		}
	}
	return latest, nil
}

func NewLog(logPath string) (*Log, error) {
	db := &Log{
		LogPath: logPath,
	}
	_, err := os.Stat(logPath)
	if os.IsNotExist(err) {
		file, err := os.Create(logPath)
		if err != nil {
			return nil, fmt.Errorf("error creating log file: %w", err)
		}
		file.Close() // Close immediately as we don't need the handle here
		return db, nil
	}
	// If file exists, attempt to compact it
	if err = compact(db); err != nil {
		return nil, fmt.Errorf("error during log compaction: %w", err)
	}
	return db, nil
}

func compact(l *Log) error {
	// Open the original log file for reading
	readFile, err := os.Open(l.LogPath)
	if err != nil {
		return fmt.Errorf("error opening file for reading: %v", err)
	}
	defer readFile.Close()

	scanner := bufio.NewScanner(readFile)
	latestValues := make(map[string]string)

	// Read all entries and keep the latest value for each key
	for scanner.Scan() {
		line := scanner.Bytes()
		var entry map[string]string
		if err := json.Unmarshal(line, &entry); err != nil {
			// Ignore malformed lines
			continue
		}
		for k, v := range entry {
			latestValues[k] = v
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
	for key, value := range latestValues {
		marshalled, err := json.Marshal(map[string]string{
			key: value,
		})
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
