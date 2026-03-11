# logfile

[![Test](https://github.com/alialaee/logfile/actions/workflows/test.yml/badge.svg)](https://github.com/alialaee/logfile/actions/workflows/test.yml)

> [!NOTE]  
> I extracted this from an in-house database engine. It's a work in progress, but I wanted to share and improve it in a separate repo. Feedback and contributions are very welcome!

`logfile` is a concurrent, append-only log file optimized for SSDs and high concurrency in Go. It

## Why

Most log file implementations flush after every write. This one batches concurrent writers into a single IO + fsync, which makes a huge difference on SSDs.

> [!WARNING]
> Performance scales with concurrency. With a single writer, you'll just get a normal fsync per write. The magic happens when many goroutines write at the same time.

The core idea is **group commit**. Multiple goroutines append records concurrently, and their writes are batched into a single `WriteAt` + `fsync`, amortizing the cost of durable IO across all waiting writers.

It uses a ping-pong buffer and a condition variable, when a flush is triggered, the active buffer is swapped with a pre-allocated "ping-pong" buffer, letting new writes to continue accumulating while the flush is in progress. Once the flush completes, all waiting writers are notified, and the next batch can be flushed.

This design provides the same durability guarantees as calling `fsync` after every individual write, but with significantly higher throughput under concurrency but not much increase in latency for individual writes.

## Features

- **Group commit:** concurrent writers are batched into one write + fsync
- **Optimized for SSDs:** minimizes write amplification and maximizes throughput with 4KB-aligned writes
- **Zero heap allocations** per write
- **Extremely safe:** no torn writes, an when a Write call returns, the data is guaranteed to be on disk
- **High concurrency:** many writers can write at the same time, and the performance scales with the number of concurrent writers
- **CRC32 checksums** for data integrity (optional)
- **Simple API:** Write() is blocking and safe (and encouraged) to call from hundreds of goroutines. No callbacks or background flushers.

## Install

```
go get github.com/alialaee/logfile
```

## Usage

```go
f, _ := os.OpenFile("my.log", os.O_CREATE|os.O_RDWR, 0644)

lf, _ := logfile.New(f, 0, 1024*1024, true) // 1MB buffer, CRC enabled

offset, _ := lf.Write(context.Background(), []byte("hello world"))

// Read back
reader := logfile.NewReader(f, 0)
data, _ := reader.ReadNext(nil)

lf.Close()
f.Close()
```

## License

MIT
