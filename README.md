# logfile

[![Test](https://github.com/alialaee/logfile/actions/workflows/test.yml/badge.svg)](https://github.com/alialaee/logfile/actions/workflows/test.yml)

> [!NOTE]  
> I extracted this from a proprietary project. It's a work in progress, but I wanted to share and improve it in a separate repo. Feedback and contributions are very welcome!

A log file optimized for SSDs and high concurrency in Go. Useful for implementing a write-ahead log.

## Why

Most log file implementations flush after every write. This one batches concurrent writers into a single IO + fsync, which makes a huge difference on SSDs.

**Important:** Performance scales with concurrency. With a single writer, you'll just get a normal fsync per write. The magic happens when many goroutines write at the same time.

## How it works

- **Group commit:** concurrent writers are batched into one write + fsync
- **4KB-aligned writes:** padded to SSD sector boundaries
- **Record framing:** each record gets a 9-byte header (marker, length, CRC32)
- **Zero heap allocations** per write
- **Ping-pong buffer:** one buffer accepts writes while the other flushes

## Install

```
go get github.com/alialaee/logfile
```

## License

MIT
