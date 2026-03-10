// Package logfile implements a log file optimized for SSD and concurrency.
// It's suitable to be used as a WAL file.
//
// ===== PLEASE NOTE: =====
// It's very important to understand that the performance goes up as the concurrency
// increases. Otherwise, the writer will be flushing after every single write.
// DO NOT use this with a single writer architecture, or you will see a normal fsync
// after every write.
//
// Design:
//   - Group Commit: multiple concurrent writers are batched into a single
//     IO write and fsync operation, maximizing SSD throughput.
//   - Aligned Writes: File appends are explicitly padded to 4KB sector boundaries
//     to optimize for SSD.
//   - Record Framing: Each record is prefixed with a 9-byte header (1 byte marker,
//     4 byte length, and 4 byte crc32).
//   - Zero heap allocations per write.
//   - Concurrent writes are serialized. Byte offsets are returned immediately, and
//     callers block until their specific offset is safely flushed to disk.
//
// The writer uses a ping-pong buffer and a condition variable to batch concurrent
// writes. The first goroutine that detects no active flush swaps the buffer and
// performs the blocking IO at 4KB aligned boundaries, while others wait for their
// specific offset to sync. The reader sequentially reads records using the 9-byte
// header and a provided buffer to avoid allocations, stopping on EOF or padding.
package logfile

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"sync"
)

const (
	alignment     = 4096 // 4KB, which is normal for SSDs
	recordMarker  = 0xAA
	maxRecordSize = 4 * 1024 * 1024 // 4MB
)

var (
	ErrInvalidRecord    = errors.New("logfile: invalid record")
	ErrCRCMismatch      = errors.New("logfile: crc checksum mismatch")
	ErrClosed           = errors.New("logfile: closed")
	ErrRecordTooLarge   = errors.New("logfile: record too large")
	ErrWriterInBadState = errors.New("logfile: writer in bad state")
)

type File interface {
	io.WriterAt
	io.ReaderAt // Needed for initial tail read.
	Sync() error
}

type LogFile struct {
	mu   sync.Mutex
	cond *sync.Cond

	file File

	buf         []byte
	pingPongBuf []byte
	maxBufSize  int

	nextOffset   int64
	syncedOffset int64

	alignedBuf []byte
	tailBuf    []byte

	withCRC    bool
	closed     bool
	isFlushing bool
	err        error
}

// New returns a LogFile.
//   - maxBufSize: threshold before applying backpressure.
//   - withCRC: CRC is optional and can be written on per record basis.
func New(file File, startOffset int64, maxBufSize int, withCRC bool) (*LogFile, error) {
	if maxBufSize <= 0 {
		return nil, errors.New("maxBufSize must be positive")
	}

	w := &LogFile{
		file:         file,
		buf:          make([]byte, 0, maxBufSize),
		pingPongBuf:  make([]byte, 0, maxBufSize),
		maxBufSize:   maxBufSize,
		nextOffset:   startOffset,
		syncedOffset: startOffset,
		alignedBuf:   make([]byte, 0, maxBufSize+alignment),
		tailBuf:      make([]byte, 0, alignment),
		withCRC:      withCRC,
	}
	w.cond = sync.NewCond(&w.mu)

	// If the existing file start offset is not block-aligned, we must
	// read the unaligned tail to properly rewrite it in the first flush.
	rem := startOffset % alignment
	if rem > 0 {
		w.tailBuf = w.tailBuf[:rem]
		physicalStart := startOffset - rem
		n, err := w.file.ReadAt(w.tailBuf, physicalStart)
		if err != nil && err != io.EOF {
			return nil, err
		}
		w.tailBuf = w.tailBuf[:n]
	}

	return w, nil
}

// Write appends data to the log, framing it with a length prefix and CRC if enabled.
// Write blocks until synced and can be called concurrently.
// It returns the starting logical file offset of the framed record.
//
// Context cancellation: If the context is canceled after data has been appended to
// the internal buffer but before the sync completes, the caller receives a context
// error. However, the data may still be flushed to disk by a concurrent flush
// operation. Callers should not assume that a context error means the write was
// not persisted.
func (w *LogFile) Write(ctx context.Context, data []byte) (int64, error) {
	if len(data) > maxRecordSize {
		return 0, ErrRecordTooLarge
	}

	if err := ctx.Err(); err != nil {
		return 0, err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.err != nil {
		return 0, ErrWriterInBadState
	}
	if w.closed {
		return 0, ErrClosed
	}

	recordLen := 9 + len(data)

	for len(w.buf) > 0 && len(w.buf)+recordLen > w.maxBufSize && w.isFlushing {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if w.closed {
			return 0, ErrClosed
		}
		if w.err != nil {
			return 0, w.err
		}
		w.wait(ctx)
	}

	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if w.closed {
		return 0, ErrClosed
	}
	if w.err != nil {
		return 0, w.err
	}

	offset := w.nextOffset

	var header [9]byte
	header[0] = recordMarker
	binary.LittleEndian.PutUint32(header[1:5], uint32(len(data)))
	var crc uint32
	if w.withCRC {
		crc = crc32.ChecksumIEEE(data)
	}
	binary.LittleEndian.PutUint32(header[5:9], crc)
	w.buf = append(w.buf, header[:]...)
	w.buf = append(w.buf, data...)

	w.nextOffset += int64(recordLen)
	targetSync := w.nextOffset

	for {
		if err := ctx.Err(); err != nil {
			return offset, err
		}
		if w.err != nil {
			return offset, w.err
		}
		if w.syncedOffset >= targetSync {
			return offset, nil
		}

		if !w.isFlushing {
			w.flushLocked()
			continue
		}

		w.wait(ctx)
	}
}

// Flush proactively flushes any pending data to disk.
func (w *LogFile) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.err != nil {
		return ErrWriterInBadState
	}
	if w.closed {
		return ErrClosed
	}

	targetSync := w.nextOffset

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if w.err != nil {
			return w.err
		}
		if w.syncedOffset >= targetSync {
			return nil
		}

		if !w.isFlushing {
			w.flushLocked()
			continue
		}

		w.wait(ctx)
	}
}

// Close flushes any pending data, rejects new writes, and waits for pending IO.
func (w *LogFile) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrClosed
	}

	w.closed = true
	w.cond.Broadcast() // Wake up any blocked writers in the first loop

	for len(w.buf) > 0 || w.isFlushing {
		if w.err != nil {
			break
		}
		if !w.isFlushing {
			w.flushLocked()
		} else {
			w.cond.Wait()
		}
	}

	return w.err
}

// flushLocked performs the IO for flushing. It MUST be called with w.mu locked.
// It unlocks w.mu to perform the blocking IO, then re-locks it before returning.
func (w *LogFile) flushLocked() {
	w.isFlushing = true
	toWrite := w.buf
	w.buf = w.pingPongBuf[:0]
	w.pingPongBuf = toWrite
	startSync := w.syncedOffset

	w.alignedBuf = w.alignedBuf[:0]
	w.alignedBuf = append(w.alignedBuf, w.tailBuf...)
	w.alignedBuf = append(w.alignedBuf, toWrite...)

	logicalEnd := len(w.alignedBuf)
	pad := (alignment - (logicalEnd % alignment)) % alignment

	if pad > 0 {
		var zeros [alignment]byte
		w.alignedBuf = append(w.alignedBuf, zeros[:pad]...)
	}

	writeOffset := startSync - int64(len(w.tailBuf))

	// Unlock to perform blocking IO
	w.mu.Unlock()

	_, err := w.file.WriteAt(w.alignedBuf, writeOffset)
	if err == nil {
		err = w.file.Sync()
	}

	// Re-acquire lock to update shared states
	w.mu.Lock()

	rem := logicalEnd % alignment
	w.tailBuf = w.tailBuf[:0]
	w.tailBuf = append(w.tailBuf, w.alignedBuf[logicalEnd-rem:logicalEnd]...)

	w.isFlushing = false
	if err != nil {
		w.err = err
	} else {
		w.syncedOffset += int64(len(toWrite))
	}

	w.cond.Broadcast()
}

// wait blocks on w.cond, but wakes up if ctx is canceled.
// It assumes w.mu is held.
func (w *LogFile) wait(ctx context.Context) {
	if ctx.Done() == nil {
		w.cond.Wait()
		return
	}

	stop := context.AfterFunc(ctx, func() {
		// We broadcast without locking the mutex.
		// If we tried to lock w.mu here, and the context was canceled while
		// waiting on the condition variable, this callback could block waiting
		// for the lock if the broadcaster is currently holding it.
		w.cond.Broadcast()
	})
	w.cond.Wait()
	stop()
}

// Reader provides sequential reading of framed LogFile records.
// Reader is not *thread-safe* unless it's wrapped in a mutex by the caller.
type Reader struct {
	file   io.ReaderAt
	offset int64
}

// NewReader creates a new Reader starting at the specified logical file offset.
// This should match an offset returned by LogFile.Write or be zero.
func NewReader(file io.ReaderAt, startOffset int64) *Reader {
	return &Reader{
		file:   file,
		offset: startOffset,
	}
}

// ReadNext reads the next record from the LogFile.
// Returns io.EOF if there are no more records or if zero-padding is encountered.
// If the provided buffer doesn't have enough capacity to hold the payload, a new,
// right-sized buffer is allocated.
func (r *Reader) ReadNext(buf []byte) ([]byte, error) {
	var header [9]byte
	n, err := r.file.ReadAt(header[:], r.offset)
	if err != nil {
		if err == io.EOF && n == 0 {
			return nil, io.EOF
		}
		if err == io.EOF {
			// Partial header read without hitting 0x00 padding means EOF happened
			// halfway into a record being written.
			return nil, io.EOF
		}
		return nil, err
	}

	if header[0] == 0x00 {
		return nil, io.EOF
	}
	if header[0] != recordMarker {
		return nil, ErrInvalidRecord
	}

	length := binary.LittleEndian.Uint32(header[1:5])
	expectedCRC := binary.LittleEndian.Uint32(header[5:9])

	if length > maxRecordSize {
		return nil, ErrInvalidRecord
	}

	if cap(buf) < int(length) {
		buf = make([]byte, length)
	} else {
		buf = buf[:length]
	}

	if length > 0 {
		n, err = r.file.ReadAt(buf, r.offset+9)
		if err != nil {
			if err == io.EOF {
				if n != int(length) {
					return nil, io.ErrUnexpectedEOF
				}
			} else {
				return nil, err
			}
		}
	}

	if expectedCRC != 0 {
		if actual := crc32.ChecksumIEEE(buf); actual != expectedCRC {
			return nil, ErrCRCMismatch
		}
	}

	r.offset += 9 + int64(length)
	return buf, nil
}

// Offset returns offset of the *next* unread record.
func (r *Reader) Offset() int64 {
	return r.offset
}
