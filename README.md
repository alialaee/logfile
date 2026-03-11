# logfile

[![Test](https://github.com/alialaee/logfile/actions/workflows/test.yml/badge.svg)](https://github.com/alialaee/logfile/actions/workflows/test.yml)

> [!NOTE]  
> I extracted this from a proprietary project. It's a work in progress, but I wanted to share and improve it in a separate repo. Feedback and contributions are very welcome!

A log file optimized for SSDs and high concurrency in Go. Useful for implementing a write-ahead log.

## Why

Most log file implementations flush after every write. This one batches concurrent writers into a single IO + fsync, which makes a huge difference on SSDs.

> [!WARNING]
> Performance scales with concurrency. With a single writer, you'll just get a normal fsync per write. The magic happens when many goroutines write at the same time.

## Features

- **Group commit:** concurrent writers are batched into one write + fsync
- **Optimized for SSDs:** minimizes write amplification and maximizes throughput with 4KB-aligned writes
- **Zero heap allocations** per write
- **Extremely safe:** no torn writes, an when a Write call returns, the data is guaranteed to be on disk
- **High concurrency:** many writers can write at the same time, and the performance scales with the number of concurrent writers

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
