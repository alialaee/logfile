package logfile

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"testing"
	"time"
)

type mockFile struct {
	mu        sync.RWMutex
	data      []byte
	nextError error
}

func newMockFile() *mockFile {
	return &mockFile{
		data: make([]byte, 0, 8192),
	}
}

func (m *mockFile) WriteAt(p []byte, off int64) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.nextError != nil {
		return 0, m.nextError
	}

	end := int(off) + len(p)
	if end > len(m.data) {
		// grow slice
		newData := make([]byte, end)
		copy(newData, m.data)
		m.data = newData
	}
	copy(m.data[off:], p)
	return len(p), nil
}

func (m *mockFile) ReadAt(p []byte, off int64) (n int, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.nextError != nil {
		return 0, m.nextError
	}

	if int(off) >= len(m.data) {
		return 0, io.EOF
	}

	n = copy(p, m.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (m *mockFile) Sync() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.nextError
}

func (m *mockFile) SetNextError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextError = err
}

func (m *mockFile) ClearNextError() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextError = nil
}

func TestBasicWriteAndRead(t *testing.T) {
	mf := newMockFile()
	w, err := New(mf, 0, 1024, true)
	if err != nil {
		t.Fatalf("Failed to create:: %v", err)
	}

	records := [][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("this is a test"),
	}

	for _, rec := range records {
		_, err := w.Write(context.Background(), rec)
		if err != nil {
			t.Fatalf("Failed to write record: %v", err)
		}
	}

	r := NewReader(mf, 0)
	var buf []byte

	for i, expected := range records {
		rec, err := r.ReadNext(buf)
		if err != nil {
			t.Fatalf("Failed to read record %d: %v", i, err)
		}
		if !bytes.Equal(rec, expected) {
			t.Errorf("Record %d mismatch: got %q, want %q", i, rec, expected)
		}
	}

	// Next read should be EOF
	_, err = r.ReadNext(buf)
	if err != io.EOF {
		t.Fatalf("Expected EOF, got: %v", err)
	}
}

func TestConcurrentWrites(t *testing.T) {
	mf := newMockFile()
	w, err := New(mf, 0, 4096, true) // Larger buffer to encourage batching
	if err != nil {
		t.Fatalf("Failed to create:: %v", err)
	}

	const numWriters = 100
	const numRecordsPerWriter = 50

	var wg sync.WaitGroup
	wg.Add(numWriters)

	for i := range numWriters {
		go func(writerID int) {
			defer wg.Done()
			for j := 0; j < numRecordsPerWriter; j++ {
				rec := bytes.Repeat([]byte{byte(writerID)}, 10) // 10 bytes payload
				_, err := w.Write(context.Background(), rec)
				if err != nil {
					t.Errorf("Writer %d failed on record %d: %v", writerID, j, err)
					return
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify all records are present.
	r := NewReader(mf, 0)
	var buf []byte
	recordCount := 0

	for {
		rec, err := r.ReadNext(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Failed to read at record %d: %v", recordCount, err)
		}

		if len(rec) != 10 {
			t.Errorf("Expected record length 10, got %d", len(rec))
		}
		recordCount++
	}

	expectedCount := numWriters * numRecordsPerWriter
	if recordCount != expectedCount {
		t.Errorf("Expected %d records, found %d", expectedCount, recordCount)
	}
}

func TestLargeMessage(t *testing.T) {
	mf := newMockFile()
	// Small maxBufSize
	w, err := New(mf, 0, 1024, true)
	if err != nil {
		t.Fatalf("Failed to create:: %v", err)
	}

	// Payload larger than maxBufSize
	largePayload := bytes.Repeat([]byte("A"), 5000)

	_, err = w.Write(context.Background(), largePayload)
	if err != nil {
		t.Fatalf("Failed to write large record: %v", err)
	}

	r := NewReader(mf, 0)
	var buf []byte
	rec, err := r.ReadNext(buf)
	if err != nil {
		t.Fatalf("Failed to read large record: %v", err)
	}
	if !bytes.Equal(rec, largePayload) {
		t.Errorf("Large record mismatch")
	}
}

func TestFileRecoveryAndAppend(t *testing.T) {
	tf, err := os.CreateTemp("", "wal_test_*.log")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer func() { _ = os.Remove(tf.Name()) }()

	// Write initial records
	w1, err := New(tf, 0, 1024, true)
	if err != nil {
		t.Fatalf("Failed to create: 1: %v", err)
	}

	rec1 := []byte("first record")
	rec2 := []byte("second record")

	_, err = w1.Write(context.Background(), rec1)
	if err != nil {
		t.Fatalf("Write 1 failed: %v", err)
	}
	off2, err := w1.Write(context.Background(), rec2)
	if err != nil {
		t.Fatalf("Write 2 failed: %v", err)
	}

	// Next sequential offset we expect to write at
	nextOff := off2 + 9 + int64(len(rec2))

	// Simulate reopening the file (like after process restart)
	_, err = tf.Seek(0, 0) // Reset fd offsets just in case, though we use ReadAt/WriteAt
	if err != nil {
		t.Fatalf("Failed to reset file offset: %v", err)
	}

	w2, err := New(tf, nextOff, 1024, true)
	if err != nil {
		t.Fatalf("Failed to create: 2: %v", err)
	}

	rec3 := []byte("third record (appended after restart)")
	_, err = w2.Write(context.Background(), rec3)
	if err != nil {
		t.Fatalf("Write 3 failed: %v", err)
	}

	// Read everything
	r := NewReader(tf, 0)
	var buf []byte

	expected := [][]byte{rec1, rec2, rec3}
	for i, exp := range expected {
		rec, err := r.ReadNext(buf)
		if err != nil {
			t.Fatalf("Failed reading record %d: %v", i+1, err)
		}
		if !bytes.Equal(rec, exp) {
			t.Errorf("Record %d mismatch: got %q, want %q", i+1, rec, exp)
		}
	}
}

func TestCRCValidation(t *testing.T) {
	mf := newMockFile()
	w, err := New(mf, 0, 1024, true)
	if err != nil {
		t.Fatalf("Failed to create:: %v", err)
	}

	_, err = w.Write(context.Background(), []byte("valid body"))
	if err != nil {
		t.Fatalf("Failed to write record: %v", err)
	}

	// Corrupt the body
	mf.mu.Lock()
	// marker(1) + length(4) + crc(4) = 9 bytes. Payload starts at 9
	mf.data[9] ^= 0xFF
	mf.mu.Unlock()

	r := NewReader(mf, 0)
	_, err = r.ReadNext(nil)
	if err != ErrCRCMismatch {
		t.Fatalf("Expected ErrCRCMismatch, got: %v", err)
	}
}

func TestClose(t *testing.T) {
	mf := newMockFile()
	w, err := New(mf, 0, 1024, false)
	if err != nil {
		t.Fatalf("Failed to create:: %v", err)
	}

	_, err = w.Write(context.Background(), []byte("tobeclosed"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	err = w.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	_, err = w.Write(context.Background(), []byte("toolate"))
	if err != ErrClosed {
		t.Fatalf("Expected ErrClosed for Write, got: %v", err)
	}

	err = w.Flush(context.Background())
	if err != ErrClosed {
		t.Fatalf("Expected ErrClosed for Flush, got: %v", err)
	}

	r := NewReader(mf, 0)
	var buf []byte
	rec, err := r.ReadNext(buf)
	if err != nil {
		t.Fatalf("Failed to read after close: %v", err)
	}
	if !bytes.Equal(rec, []byte("tobeclosed")) {
		t.Errorf("Record mismatch")
	}

	err = w.Close()
	if err != ErrClosed {
		t.Fatalf("Expected ErrClosed for subsequent Close(), got: %v", err)
	}
}

func TestContextCancellation(t *testing.T) {
	mf := newMockFile()
	// very small buffer size
	w, err := New(mf, 0, 10, false)
	if err != nil {
		t.Fatalf("Failed to create:: %v", err)
	}

	// This is a bit of a trick to pause the flush logic
	// We'll acquire the mutex manually and block the condition variable loop
	w.mu.Lock()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	errCh := make(chan error)
	go func() {
		// Because w.mu is locked by the main test goroutine, this Write will block
		// trying to acquire the mutex in w.Write, BUT that proves it handles context gracefully.
		// Actually, wait, the ctx.Err() is checked at the VERY BEGINNING.
		// If it's already expired, it returns immediately. That's one code path.
		_, writeErr := w.Write(ctx, []byte("some big payload that needs flush"))
		errCh <- writeErr
	}()

	// sleep for the timeout
	time.Sleep(100 * time.Millisecond)
	w.mu.Unlock()

	err = <-errCh
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("Expected context deadline exceeded/canceled error, got: %v", err)
	}
}

func TestErrorState(t *testing.T) {
	mf := newMockFile()
	w, err := New(mf, 0, 1024, false)
	if err != nil {
		t.Fatalf("Failed to create:: %v", err)
	}

	testErr := errors.New("simulated error")
	mf.SetNextError(testErr)

	_, err = w.Write(context.Background(), []byte("test"))
	if !errors.Is(err, testErr) {
		t.Fatalf("Expected ErrWriterInBadState, got: %v", err)
	}

	err = w.Flush(context.Background())
	if !errors.Is(err, ErrWriterInBadState) {
		t.Fatalf("Expected ErrWriterInBadState, got: %v", err)
	}

	// Clear the file error and verify we still in a bad state.
	mf.ClearNextError()
	_, err = w.Write(context.Background(), []byte("test after clear"))
	if !errors.Is(err, ErrWriterInBadState) {
		t.Fatalf("Expected ErrWriterInBadState, got: %v", err)
	}
}

func TestRecordExceedsMaxSize(t *testing.T) {
	mf := newMockFile()
	w, err := New(mf, 0, 1024, false)
	if err != nil {
		t.Fatalf("Failed to create:: %v", err)
	}

	largePayload := bytes.Repeat([]byte("X"), 4*1024*1024+1)
	_, err = w.Write(context.Background(), largePayload)
	if err == nil {
		t.Fatalf("Expected error for record exceeding max size, got nil")
	}
}

func TestCloseWhileWritesInFlight(t *testing.T) {
	mf := newMockFile()
	w, err := New(mf, 0, 4096, false)
	if err != nil {
		t.Fatalf("Failed to create:: %v", err)
	}

	const numWriters = 50
	var wg sync.WaitGroup
	wg.Add(numWriters)

	writeStarted := make(chan struct{})
	var startOnce sync.Once

	for i := range numWriters {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				rec := bytes.Repeat([]byte{byte(id)}, 10)
				_, writeErr := w.Write(context.Background(), rec)
				if writeErr != nil {
					// After close, writes should fail with ErrClosed
					if !errors.Is(writeErr, ErrClosed) {
						t.Errorf("Writer %d: expected ErrClosed or nil, got: %v", id, writeErr)
					}
					return
				}
				startOnce.Do(func() { close(writeStarted) })
			}
		}(i)
	}

	// Wait for at least one write to start, then close
	<-writeStarted
	time.Sleep(5 * time.Millisecond) // Let it queue up

	err = w.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	wg.Wait()

	r := NewReader(mf, 0)
	count := 0
	for {
		_, readErr := r.ReadNext(nil)
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			t.Fatalf("Failed to read record %d: %v", count, readErr)
		}
		count++
	}

	if count == 0 {
		t.Fatal("Zero records has been read")
	}
	t.Logf("Successfully read %d records after Close() with concurrent writers", count)
}
