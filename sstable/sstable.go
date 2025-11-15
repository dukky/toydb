package sstable

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

const (
	// IndexInterval defines how often we create an index entry (every Nth key)
	IndexInterval = 16
	// SSTableVersion is the file format version
	SSTableVersion = 1
)

// IndexEntry represents an entry in the sparse index
type IndexEntry struct {
	Key    string
	Offset int64
}

// SSTableFooter contains metadata about the SSTable
type SSTableFooter struct {
	Version     int
	IndexOffset int64 // Byte offset where the index starts
	NumEntries  int   // Total number of data entries
}

// SSTable represents a sorted string table on disk
type SSTable struct {
	FilePath string
	index    []IndexEntry    // Sparse index loaded in memory
	footer   SSTableFooter
}

// WriteSSTable writes a sorted list of entries to disk as an SSTable
func WriteSSTable(filePath string, entries []Entry) error {
	// Create temporary file for atomic write
	tempPath := filePath + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create SSTable file: %w", err)
	}

	// Ensure entries are sorted
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key < entries[j].Key
	})

	// Write data entries and build sparse index
	var index []IndexEntry
	var offset int64 = 0

	for i, entry := range entries {
		// Record index entry every IndexInterval entries
		if i%IndexInterval == 0 {
			index = append(index, IndexEntry{
				Key:    entry.Key,
				Offset: offset,
			})
		}

		// Serialize entry to JSON
		data, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("failed to marshal entry: %w", err)
		}

		// Write length prefix (8 bytes)
		length := int64(len(data))
		if err := binary.Write(file, binary.LittleEndian, length); err != nil {
			return fmt.Errorf("failed to write entry length: %w", err)
		}
		offset += 8

		// Write data
		n, err := file.Write(data)
		if err != nil {
			return fmt.Errorf("failed to write entry data: %w", err)
		}
		offset += int64(n)
	}

	// Record the start of the index section
	indexOffset := offset

	// Write sparse index
	for _, idxEntry := range index {
		data, err := json.Marshal(idxEntry)
		if err != nil {
			return fmt.Errorf("failed to marshal index entry: %w", err)
		}

		// Write length prefix
		length := int64(len(data))
		if err := binary.Write(file, binary.LittleEndian, length); err != nil {
			return fmt.Errorf("failed to write index entry length: %w", err)
		}

		// Write data
		if _, err := file.Write(data); err != nil {
			return fmt.Errorf("failed to write index entry data: %w", err)
		}
	}

	// Write footer
	footer := SSTableFooter{
		Version:     SSTableVersion,
		IndexOffset: indexOffset,
		NumEntries:  len(entries),
	}
	footerData, err := json.Marshal(footer)
	if err != nil {
		return fmt.Errorf("failed to marshal footer: %w", err)
	}

	// Write footer data first
	if _, err := file.Write(footerData); err != nil {
		return fmt.Errorf("failed to write footer data: %w", err)
	}

	// Write footer length last (so we can find it at the end of the file)
	footerLength := int64(len(footerData))
	if err := binary.Write(file, binary.LittleEndian, footerLength); err != nil {
		return fmt.Errorf("failed to write footer length: %w", err)
	}

	// Sync to disk
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync SSTable file: %w", err)
	}

	// Close before rename
	file.Close()

	// Atomic rename
	if err := os.Rename(tempPath, filePath); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to rename SSTable file: %w", err)
	}

	return nil
}

// OpenSSTable opens an existing SSTable and loads its index into memory
func OpenSSTable(filePath string) (*SSTable, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open SSTable: %w", err)
	}
	defer file.Close()

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat SSTable: %w", err)
	}
	fileSize := stat.Size()

	if fileSize < 8 {
		return nil, fmt.Errorf("SSTable file too small")
	}

	// Read footer length (last 8 bytes)
	if _, err := file.Seek(fileSize-8, 0); err != nil {
		return nil, fmt.Errorf("failed to seek to footer length: %w", err)
	}

	var footerLength int64
	if err := binary.Read(file, binary.LittleEndian, &footerLength); err != nil {
		return nil, fmt.Errorf("failed to read footer length: %w", err)
	}

	// Read footer
	footerOffset := fileSize - 8 - footerLength
	if footerOffset < 0 {
		return nil, fmt.Errorf("invalid footer offset: %d (fileSize=%d, footerLength=%d)", footerOffset, fileSize, footerLength)
	}
	if _, err := file.Seek(footerOffset, 0); err != nil {
		return nil, fmt.Errorf("failed to seek to footer: %w", err)
	}

	footerData := make([]byte, footerLength)
	if _, err := file.Read(footerData); err != nil {
		return nil, fmt.Errorf("failed to read footer: %w", err)
	}

	var footer SSTableFooter
	if err := json.Unmarshal(footerData, &footer); err != nil {
		return nil, fmt.Errorf("failed to unmarshal footer: %w", err)
	}

	// Read sparse index
	if _, err := file.Seek(footer.IndexOffset, 0); err != nil {
		return nil, fmt.Errorf("failed to seek to index: %w", err)
	}

	var index []IndexEntry
	currentOffset := footer.IndexOffset

	for currentOffset < footerOffset {
		// Read index entry length
		var length int64
		if err := binary.Read(file, binary.LittleEndian, &length); err != nil {
			return nil, fmt.Errorf("failed to read index entry length: %w", err)
		}
		currentOffset += 8

		// Read index entry data
		data := make([]byte, length)
		if _, err := file.Read(data); err != nil {
			return nil, fmt.Errorf("failed to read index entry data: %w", err)
		}
		currentOffset += length

		var idxEntry IndexEntry
		if err := json.Unmarshal(data, &idxEntry); err != nil {
			return nil, fmt.Errorf("failed to unmarshal index entry: %w", err)
		}

		index = append(index, idxEntry)
	}

	return &SSTable{
		FilePath: filePath,
		index:    index,
		footer:   footer,
	}, nil
}

// Get retrieves a value by key from the SSTable
// Returns (value, exists, error) where exists=true even for tombstones
// Check if value is empty and exists=true to detect tombstones
func (sst *SSTable) Get(key string) (string, bool, error) {
	file, err := os.Open(sst.FilePath)
	if err != nil {
		return "", false, fmt.Errorf("failed to open SSTable: %w", err)
	}
	defer file.Close()

	// Find the index entry to start scanning from
	startOffset := int64(0)
	endOffset := sst.footer.IndexOffset

	// Binary search in sparse index to find starting point
	if len(sst.index) > 0 {
		idx := sort.Search(len(sst.index), func(i int) bool {
			return sst.index[i].Key >= key
		})

		if idx < len(sst.index) {
			if sst.index[idx].Key == key {
				// Direct hit in index
				startOffset = sst.index[idx].Offset
			} else if idx > 0 {
				// Start from previous index entry
				startOffset = sst.index[idx-1].Offset
			}
			// Set end offset to next index entry if exists
			if idx+1 < len(sst.index) {
				endOffset = sst.index[idx+1].Offset
			}
		} else if len(sst.index) > 0 {
			// Key might be after last index entry
			startOffset = sst.index[len(sst.index)-1].Offset
		}
	}

	// Scan from startOffset to endOffset
	if _, err := file.Seek(startOffset, 0); err != nil {
		return "", false, fmt.Errorf("failed to seek: %w", err)
	}

	currentOffset := startOffset
	for currentOffset < endOffset {
		// Read entry length
		var length int64
		if err := binary.Read(file, binary.LittleEndian, &length); err != nil {
			// End of data section
			break
		}
		currentOffset += 8

		// Read entry data
		data := make([]byte, length)
		if _, err := file.Read(data); err != nil {
			return "", false, fmt.Errorf("failed to read entry: %w", err)
		}
		currentOffset += length

		var entry Entry
		if err := json.Unmarshal(data, &entry); err != nil {
			return "", false, fmt.Errorf("failed to unmarshal entry: %w", err)
		}

		// Check if we found the key
		if entry.Key == key {
			if entry.Deleted {
				// Return exists=true for tombstones so caller knows to stop searching
				return "", true, nil
			}
			return entry.Value, true, nil
		}

		// Since entries are sorted, if we passed the key, it doesn't exist
		if entry.Key > key {
			return "", false, nil
		}
	}

	return "", false, nil
}

// GetAllEntries returns all entries in the SSTable
func (sst *SSTable) GetAllEntries() ([]Entry, error) {
	file, err := os.Open(sst.FilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open SSTable: %w", err)
	}
	defer file.Close()

	var entries []Entry
	var currentOffset int64 = 0

	for currentOffset < sst.footer.IndexOffset {
		// Read entry length
		var length int64
		if err := binary.Read(file, binary.LittleEndian, &length); err != nil {
			break
		}
		currentOffset += 8

		// Read entry data
		data := make([]byte, length)
		if _, err := file.Read(data); err != nil {
			return nil, fmt.Errorf("failed to read entry: %w", err)
		}
		currentOffset += length

		var entry Entry
		if err := json.Unmarshal(data, &entry); err != nil {
			return nil, fmt.Errorf("failed to unmarshal entry: %w", err)
		}

		entries = append(entries, entry)
	}

	return entries, nil
}
