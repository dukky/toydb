# Performance Comparison: SSTable vs Log Storage

Benchmark results comparing the Log-based and SSTable storage engines.

## Benchmark Results

### Write Performance

| Storage Engine | Time per op | Memory per op | Allocations |
|---------------|-------------|---------------|-------------|
| **Log** | 2,354 µs | 279 B | 8 |
| **SSTable** | 19 µs | 500 B | 7 |

**Winner: SSTable (121x faster)** ✓

The SSTable writes are dramatically faster because writes go to an in-memory memtable, while the log engine syncs to disk on every write.

### Read Performance (Scaled by Dataset Size)

#### 100 Entries
| Storage Engine | Time per op | Memory per op |
|---------------|-------------|---------------|
| **Log** | 252 µs | 32,291 B |
| **SSTable** | 248 µs | 3,068 B |

**Similar performance** (~2% difference)

#### 1,000 Entries
| Storage Engine | Time per op | Memory per op |
|---------------|-------------|---------------|
| **Log** | 1,187 µs | 284,468 B |
| **SSTable** | 250 µs | 3,180 B |

**Winner: SSTable (4.7x faster)** ✓

#### 10,000 Entries
| Storage Engine | Time per op | Memory per op |
|---------------|-------------|---------------|
| **Log** | 10,618 µs | 2,878,205 B |
| **SSTable** | 260 µs | 3,191 B |

**Winner: SSTable (40.8x faster)** ✓

### Key Insight: Read Scaling

The SSTable read time stays **constant** (~250 µs) regardless of dataset size, while the Log read time grows **linearly** with the number of entries:

- Log: O(n) - must scan entire file
- SSTable: O(log n) - uses sparse index + binary search

```
Read Time by Dataset Size:
┌──────────────────────────────────────────────────────┐
│ 12ms │                                          ●Log   │
│ 10ms │                                        ●        │
│  8ms │                                      ●          │
│  6ms │                                    ●            │
│  4ms │                                  ●              │
│  2ms │                                ●                │
│  0ms │  ■───────────■───────────■                      │
│      └────────────────────────────────────────────────┤
│         100         1,000       10,000                 │
│                   Dataset Size                         │
│  ■ SSTable (flat)      ● Log (linear growth)           │
└────────────────────────────────────────────────────────┘
```

### Mixed Workload (50% reads, 50% writes on 1,000 entries)

| Storage Engine | Time per op | Memory per op | Allocations |
|---------------|-------------|---------------|-------------|
| **Log** | 2,091 µs | 178,582 B | 4,384 |
| **SSTable** | 22 µs | 234 B | 6 |

**Winner: SSTable (95x faster)** ✓

The SSTable engine excels at mixed workloads due to:
- Fast in-memory writes
- Efficient indexed reads
- Minimal memory allocation

### Storage Efficiency (10,000 entries)

| Storage Engine | Disk Size | Notes |
|---------------|-----------|-------|
| **Log** | 369 KB | Single append-only file |
| **SSTable** | 618 KB | Multiple SSTables + indices |

**Ratio: 1.68x** (SSTable uses ~68% more disk space)

The SSTable uses more disk space due to:
- Sparse index overhead (every 16th key)
- Footer metadata
- Multiple SSTable files before compaction

However, this overhead buys:
- Dramatically faster reads
- Better memory efficiency
- Predictable performance at scale

## Performance Characteristics Summary

### Log Storage
**Best for:**
- Write-heavy workloads where reads are rare
- Small datasets (< 1,000 entries)
- Simplicity and minimal disk usage

**Limitations:**
- Read performance degrades linearly with dataset size
- High memory usage for reads (must scan entire file)
- Compaction is disruptive (full rewrite)

### SSTable Storage
**Best for:**
- Read-heavy or mixed workloads
- Large datasets (> 1,000 entries)
- Applications requiring predictable latency

**Advantages:**
- Constant-time reads regardless of dataset size
- 40-120x faster for typical workloads
- Efficient memory usage
- Non-disruptive background compaction

**Trade-offs:**
- ~68% more disk space
- Slightly more complex implementation

## Recommendations

**Choose Log if:**
- You have a small dataset (< 1,000 entries)
- Writes are infrequent and reads are very rare
- Disk space is extremely constrained
- You need the simplest possible implementation

**Choose SSTable if:**
- You have more than 1,000 entries
- You need to read data more than once
- Performance predictability matters
- You're building a real database application

For most use cases with more than a few hundred entries, **SSTable is the clear winner**.

## Real-World Impact

With 10,000 entries:
- **Log**: 10.6ms per read (suitable for batch processing)
- **SSTable**: 0.26ms per read (suitable for interactive applications)

For a web application doing 100 requests/second:
- **Log**: Would spend 1.06 seconds just reading data (impossible)
- **SSTable**: Would spend 0.026 seconds reading data (2.6% of time)
