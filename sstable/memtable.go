package sstable

import (
	"sort"
	"sync"
)

// Entry represents a key-value pair in the memtable
type Entry struct {
	Key     string
	Value   string
	Deleted bool
}

// Memtable is an in-memory sorted data structure for storing key-value pairs
type Memtable struct {
	mu      sync.RWMutex
	entries map[string]Entry
	size    int // Approximate size in bytes
}

// NewMemtable creates a new empty memtable
func NewMemtable() *Memtable {
	return &Memtable{
		entries: make(map[string]Entry),
		size:    0,
	}
}

// Put adds or updates a key-value pair in the memtable
func (m *Memtable) Put(key, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Remove old entry size if exists
	if old, exists := m.entries[key]; exists {
		m.size -= len(old.Key) + len(old.Value)
	}

	entry := Entry{
		Key:     key,
		Value:   value,
		Deleted: false,
	}
	m.entries[key] = entry
	m.size += len(key) + len(value)
}

// Get retrieves a value by key from the memtable
func (m *Memtable) Get(key string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, exists := m.entries[key]
	if !exists || entry.Deleted {
		return "", false
	}
	return entry.Value, true
}

// Delete marks a key as deleted (tombstone)
func (m *Memtable) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Remove old entry size if exists
	if old, exists := m.entries[key]; exists {
		m.size -= len(old.Key) + len(old.Value)
	}

	entry := Entry{
		Key:     key,
		Value:   "",
		Deleted: true,
	}
	m.entries[key] = entry
	m.size += len(key)
}

// Size returns the approximate size of the memtable in bytes
func (m *Memtable) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

// IsEmpty returns true if the memtable has no entries
func (m *Memtable) IsEmpty() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.entries) == 0
}

// GetSortedEntries returns all entries sorted by key
func (m *Memtable) GetSortedEntries() []Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Get sorted keys
	keys := make([]string, 0, len(m.entries))
	for key := range m.entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Build sorted entries slice
	entries := make([]Entry, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, m.entries[key])
	}

	return entries
}

// Clear removes all entries from the memtable
func (m *Memtable) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = make(map[string]Entry)
	m.size = 0
}
