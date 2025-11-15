# toydb

Some very basic database engine implementations, written as a learning exercise while reading [Designing Data-Intensive Applications](https://dataintensive.net/)

## Storage Engines

ToyDB implements three different storage engines:

### 1. Log-based Storage (`log`)
- **Architecture**: Append-only write-ahead log (WAL)
- **File Format**: Line-delimited JSON
- **Reads**: O(n) - sequential scan through entire log
- **Writes**: O(1) - append to end of file
- **Features**:
  - Tombstone-based deletion
  - Automatic and manual compaction
  - Atomic operations with fsync

### 2. Hash Index Storage (`hash`)
- **Architecture**: Direct-access hash table with byte-offset index
- **File Format**: Length-prefixed binary entries
- **Reads**: O(1) - direct lookup via in-memory index
- **Writes**: O(1) - append with index update
- **Limitations**: Delete operations not yet implemented

### 3. SSTable Storage (`sstable`)
- **Architecture**: Log-Structured Merge-Tree (LSM-Tree) with SSTables
- **Components**:
  - **Memtable**: In-memory sorted structure (red-black tree)
  - **SSTables**: Sorted on-disk tables with sparse index
- **File Format**:
  - Sorted key-value entries (JSON)
  - Sparse index (every 16th key)
  - Footer with metadata
- **Reads**: O(log n) - binary search in sparse index + sequential scan
- **Writes**: O(log n) - write to memtable, flush when threshold reached
- **Features**:
  - Automatic memtable flushing (1MB threshold)
  - Automatic compaction (when 4+ SSTables exist)
  - Manual flush and compaction operations
  - Tombstone-based deletion with proper multi-SSTable handling
  - Data persistence across restarts

## Usage

Build the project:
```bash
go build -o toydb
```

### Write Operation
```bash
./toydb -type=sstable -file=/path/to/data -op=write -key=mykey -value=myvalue
```

### Read Operation
```bash
./toydb -type=sstable -file=/path/to/data -op=read -key=mykey
```

### Delete Operation
```bash
./toydb -type=sstable -file=/path/to/data -op=delete -key=mykey
```

### Flush (SSTable only)
Manually flush memtable to disk:
```bash
./toydb -type=sstable -file=/path/to/data -op=flush
```

### Compact
Merge SSTables and remove tombstones:
```bash
./toydb -type=sstable -file=/path/to/data -op=compact
```

## Running Tests

```bash
# Test all packages
go test ./...

# Test specific package with verbose output
go test ./sstable/... -v
```

## Architecture Details

### SSTable Implementation

The SSTable implementation follows the LSM-Tree design pattern:

1. **Write Path**:
   - Writes go to in-memory memtable
   - When memtable reaches 1MB, it's flushed to disk as a new SSTable
   - Each SSTable is immutable once written
   - Automatic compaction triggered when 4+ SSTables exist

2. **Read Path**:
   - Check memtable first (O(log n))
   - Check SSTables from newest to oldest
   - Use sparse index for efficient lookup within each SSTable
   - Stop at first match (including tombstones)

3. **Compaction**:
   - Merges all SSTables into a single sorted table
   - Removes deleted entries (tombstones)
   - Keeps only the latest value for each key
   - Reduces read amplification and disk usage

4. **File Format**:
   ```
   [Data Section]
   Entry 1: [8 bytes length][JSON entry]
   Entry 2: [8 bytes length][JSON entry]
   ...

   [Sparse Index Section]
   Index Entry 1: [8 bytes length][JSON index entry]
   Index Entry 2: [8 bytes length][JSON index entry]
   ...

   [Footer]
   [JSON footer data]
   [8 bytes footer length]
   ```
