package main

import (
	"fmt"
	"os"
	"testing"

	logdb "github.com/dukky/toydb/log"
	"github.com/dukky/toydb/sstable"
)

// Benchmark sequential writes
func BenchmarkLogWrites(b *testing.B) {
	tmpDir := b.TempDir()
	logPath := tmpDir + "/bench.log"

	db, err := logdb.NewLog(logPath)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		if err := db.Write(key, value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSSTableWrites(b *testing.B) {
	tmpDir := b.TempDir()

	db, err := sstable.NewSSTableDB(tmpDir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		if err := db.Write(key, value); err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark reads after writing N entries
func benchmarkLogReads(b *testing.B, numEntries int) {
	tmpDir := b.TempDir()
	logPath := tmpDir + "/bench.log"

	db, err := logdb.NewLog(logPath)
	if err != nil {
		b.Fatal(err)
	}

	// Populate with data
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		if err := db.Write(key, value); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Read a random key (use modulo to stay in range)
		key := fmt.Sprintf("key%d", i%numEntries)
		if _, err := db.Read(key); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkSSTableReads(b *testing.B, numEntries int) {
	tmpDir := b.TempDir()

	db, err := sstable.NewSSTableDB(tmpDir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Populate with data
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		if err := db.Write(key, value); err != nil {
			b.Fatal(err)
		}
	}

	// Flush to ensure data is on disk
	if err := db.Flush(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Read a random key (use modulo to stay in range)
		key := fmt.Sprintf("key%d", i%numEntries)
		if _, err := db.Read(key); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLogReads_100(b *testing.B)    { benchmarkLogReads(b, 100) }
func BenchmarkLogReads_1000(b *testing.B)   { benchmarkLogReads(b, 1000) }
func BenchmarkLogReads_10000(b *testing.B)  { benchmarkLogReads(b, 10000) }

func BenchmarkSSTableReads_100(b *testing.B)    { benchmarkSSTableReads(b, 100) }
func BenchmarkSSTableReads_1000(b *testing.B)   { benchmarkSSTableReads(b, 1000) }
func BenchmarkSSTableReads_10000(b *testing.B)  { benchmarkSSTableReads(b, 10000) }

// Benchmark mixed workload (50% reads, 50% writes)
func BenchmarkLogMixed(b *testing.B) {
	tmpDir := b.TempDir()
	logPath := tmpDir + "/bench.log"

	db, err := logdb.NewLog(logPath)
	if err != nil {
		b.Fatal(err)
	}

	// Prepopulate with some data
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		if err := db.Write(key, value); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i%1000)
		if i%2 == 0 {
			// Write
			value := fmt.Sprintf("newvalue%d", i)
			if err := db.Write(key, value); err != nil {
				b.Fatal(err)
			}
		} else {
			// Read
			if _, err := db.Read(key); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkSSTableMixed(b *testing.B) {
	tmpDir := b.TempDir()

	db, err := sstable.NewSSTableDB(tmpDir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Prepopulate with some data
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		if err := db.Write(key, value); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i%1000)
		if i%2 == 0 {
			// Write
			value := fmt.Sprintf("newvalue%d", i)
			if err := db.Write(key, value); err != nil {
				b.Fatal(err)
			}
		} else {
			// Read
			if _, err := db.Read(key); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// Test file size growth
func TestFileSizeComparison(t *testing.T) {
	numEntries := 10000

	// Test Log DB
	tmpDir1 := t.TempDir()
	logPath := tmpDir1 + "/size_test.log"
	logDB, err := logdb.NewLog(logPath)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		if err := logDB.Write(key, value); err != nil {
			t.Fatal(err)
		}
	}

	logStat, err := os.Stat(logPath)
	if err != nil {
		t.Fatal(err)
	}
	logSize := logStat.Size()

	// Test SSTable DB
	tmpDir2 := t.TempDir()
	sstDB, err := sstable.NewSSTableDB(tmpDir2)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		if err := sstDB.Write(key, value); err != nil {
			t.Fatal(err)
		}
	}

	if err := sstDB.Close(); err != nil {
		t.Fatal(err)
	}

	// Calculate SSTable total size
	entries, err := os.ReadDir(tmpDir2)
	if err != nil {
		t.Fatal(err)
	}

	var sstSize int64
	for _, entry := range entries {
		if !entry.IsDir() {
			info, err := entry.Info()
			if err == nil {
				sstSize += info.Size()
			}
		}
	}

	t.Logf("Log DB size: %d bytes (%.2f KB)", logSize, float64(logSize)/1024)
	t.Logf("SSTable DB size: %d bytes (%.2f KB)", sstSize, float64(sstSize)/1024)
	t.Logf("Size ratio: %.2fx", float64(sstSize)/float64(logSize))
}
