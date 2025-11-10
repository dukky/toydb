package hashkv

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
)

type HashKVEntry struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Deleted bool   `json:"deleted"`
}

type HashKV struct {
	logPath         string
	byteOffsetIndex map[string]int64
}

func (h *HashKV) Write(key string, value string) error {
	file, err := os.OpenFile(h.logPath, os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("error opening logPath: %v", err)
	}

	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	entry := HashKVEntry{
		Key:     key,
		Value:   value,
		Deleted: false,
	}

	entryBytes, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("error marshalling entry: %v", err)
	}

	length := int64(len(entryBytes))
	if err := binary.Write(file, binary.LittleEndian, length); err != nil {
		return fmt.Errorf("error writing length: %v", err)
	}
	if err := binary.Write(file, binary.LittleEndian, entryBytes); err != nil {
		return fmt.Errorf("error writing entry: %v", err)
	}

	h.byteOffsetIndex[key] = offset

	return nil
}

func (h *HashKV) Read(key string) (string, error) {
	offset, ok := h.byteOffsetIndex[key]
	if !ok {
		return "", fmt.Errorf("key not found")
	}

	file, err := os.Open(h.logPath)
	if err != nil {
		return "", fmt.Errorf("error opening logPath: %v", err)
	}
	defer file.Close()

	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		return "", fmt.Errorf("error seeking: %v", err)
	}

	length := int64(0)
	err = binary.Read(file, binary.LittleEndian, &length)
	if err != nil {
		return "", fmt.Errorf("error reading length: %v", err)
	}

	data := make([]byte, length)
	err = binary.Read(file, binary.LittleEndian, data)
	if err != nil {
		return "", fmt.Errorf("error reading data: %v", err)
	}

	// Try to unmarshal as HashKVEntry (new format)
	var entry HashKVEntry
	if err := json.Unmarshal(data, &entry); err == nil && entry.Key != "" {
		// New format
		return entry.Value, nil
	}

	// Fall back to old format: {"key":"value"}
	var oldFormat map[string]string
	if err := json.Unmarshal(data, &oldFormat); err != nil {
		return "", fmt.Errorf("error unmarshalling entry: %v", err)
	}

	// Return the value for the requested key from old format
	if value, exists := oldFormat[key]; exists {
		return value, nil
	}

	return "", fmt.Errorf("key not found in entry")
}

func (h *HashKV) Delete(key string) error {
	file, err := os.OpenFile(h.logPath, os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("error opening logPath: %v", err)
	}
	defer file.Close()

	_, err = file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("error seeking to end: %v", err)
	}

	tombstone := HashKVEntry{
		Key:     key,
		Value:   "",
		Deleted: true,
	}

	tombstoneBytes, err := json.Marshal(tombstone)
	if err != nil {
		return fmt.Errorf("error marshalling tombstone: %v", err)
	}

	length := int64(len(tombstoneBytes))
	if err := binary.Write(file, binary.LittleEndian, length); err != nil {
		return fmt.Errorf("error writing length: %v", err)
	}
	if err := binary.Write(file, binary.LittleEndian, tombstoneBytes); err != nil {
		return fmt.Errorf("error writing tombstone: %v", err)
	}

	if err := file.Sync(); err != nil {
		return fmt.Errorf("error syncing file: %v", err)
	}

	// Remove key from index so Read will return error for deleted keys
	delete(h.byteOffsetIndex, key)

	return nil
}

func NewHashKV(logPath string) *HashKV {
	hashKV := &HashKV{
		logPath:         logPath,
		byteOffsetIndex: map[string]int64{},
	}

	_, err := os.Stat(logPath)
	if os.IsNotExist(err) {
		file, err := os.Create(logPath)
		if err != nil {
			log.Fatal("Failed to create file: ", err)
		}
		defer file.Close()
		return hashKV
	}

	// Process the file if it exists, and fill the index
	// We need to track the latest entry for each key
	file, err := os.Open(logPath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Map to track latest position and deletion status for each key
	latestEntries := make(map[string]struct {
		position int64
		deleted  bool
	})

	for {
		length := int64(0)
		pos, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		err = binary.Read(file, binary.LittleEndian, &length)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		data := make([]byte, length)
		err = binary.Read(file, binary.LittleEndian, &data)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}

		// Try to unmarshal as HashKVEntry (new format)
		var entry HashKVEntry
		if err := json.Unmarshal(data, &entry); err == nil && entry.Key != "" {
			// New format - track latest position and deletion status
			latestEntries[entry.Key] = struct {
				position int64
				deleted  bool
			}{position: pos, deleted: entry.Deleted}
			continue
		}

		// Fall back to old format: {"key":"value"}
		unmarshalled := make(map[string]any)
		err = json.Unmarshal(data, &unmarshalled)
		if err != nil {
			log.Fatal(err)
		}
		keys := make([]string, 0, len(unmarshalled))
		for k := range unmarshalled {
			keys = append(keys, k)
		}
		// Old format entries are never deleted
		latestEntries[keys[0]] = struct {
			position int64
			deleted  bool
		}{position: pos, deleted: false}
	}

	// Now build the index from latest entries, excluding deleted ones
	for key, entry := range latestEntries {
		if !entry.deleted {
			hashKV.byteOffsetIndex[key] = entry.position
		}
	}

	return hashKV
}
