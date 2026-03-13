# Benchmarking

The benchmarking is a work in progress, but the initial results are promising. The benchmark writes 50 records of random sizes between 512 bytes and 1MB, with a concurrency of 20 goroutines (each writing the same set of records), and measures the total time taken for all writes to complete. If the library doesn't support concurrent writes, it will just write the records sequentially in a single goroutine.


# Libraries

- **Logfile**: the library implemented in this repo.
- **Direct File Write+Flush**: a simple implementation that writes each record directly to the file and calls `Sync` after each write, without any batching or optimization.
- **Tidwall WAL**: a popular write-ahead log library that supports concurrent writes and fsync, but doesn't use group commit.
- **Hashicorp Raft**: the write-ahead log implementation used in Hashicorp's Raft library, which also supports concurrent writes but doesn't use group commit.
- **Pebble Record**: the record writer used in the Pebble key-value store.


## Results

Here's the initial benchmark results on my machine:

```
=== Logfile Write ===
Total bytes written: 477 MB
Time taken: 576.585416ms
Throughput: 828.85 MB/s

=== Direct File Write+Flush ===
Total bytes written: 119 MB
Time taken: 1.022756167s
Throughput: 116.82 MB/s

=== Tidwall WAL Write ===
Total bytes written: 119 MB
Time taken: 1.117973042s
Throughput: 106.87 MB/s

=== Hashicorp Raft Write ===
Total bytes written: 119 MB
Time taken: 1.197531459s
Throughput: 99.77 MB/s

=== Pebble Record Write ===
Total bytes written: 477 MB
Time taken: 678.86125ms
Throughput: 703.98 MB/s
```