package logfile

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
)

type noopFile struct {
	offset int64
}

func (n *noopFile) WriteAt(p []byte, off int64) (int, error) {
	n.offset += int64(len(p))
	return len(p), nil
}

func (n *noopFile) ReadAt(p []byte, off int64) (int, error) {
	return 0, io.EOF
}

func (n *noopFile) Sync() error {
	return nil
}

func BenchmarkWriteMemory(b *testing.B) {
	w, err := New(&noopFile{}, 0, 1024*1024, false)
	if err != nil {
		b.Fatalf("Failed to create LogFile: %v", err)
	}

	payload := bytes.Repeat([]byte("BenchData"), 10) // 90 bytes

	b.ReportAllocs()

	for b.Loop() {
		_, err := w.Write(context.Background(), payload)
		if err != nil {
			b.Fatalf("Write failed: %v", err)
		}
	}
}

func BenchmarkWriteConcurrentMemory(b *testing.B) {
	w, err := New(&noopFile{}, 0, 1024*1024*10, false)
	if err != nil {
		b.Fatalf("Failed to create WAL: %v", err)
	}

	payload := bytes.Repeat([]byte("BenchData"), 10)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := w.Write(context.Background(), payload)
			if err != nil {
				b.Fatalf("Write failed: %v", err)
			}
		}
	})
}

func BenchmarkWriteRealDisk(b *testing.B) {
	f, err := os.CreateTemp("", "wal_bench_*.log")
	if err != nil {
		b.Fatalf("failed to create temp file: %v", err)
	}
	defer func() { _ = os.Remove(f.Name()) }()

	w, err := New(f, 0, 1024*1024*2, false) // 2MB batch size
	if err != nil {
		b.Fatalf("Failed to create LogFile: %v", err)
	}

	payload := bytes.Repeat([]byte("BenchData"), 10) // 90 bytes

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))

	for b.Loop() {
		_, err := w.Write(context.Background(), payload)
		if err != nil {
			b.Fatalf("Write failed: %v", err)
		}
	}
}

func BenchmarkWriteConcurrentRealDisk(b *testing.B) {
	f, err := os.CreateTemp("", "wal_bench_sync_*.log")
	if err != nil {
		b.Fatalf("failed to create temp file: %v", err)
	}
	defer func() { _ = os.Remove(f.Name()) }()

	w, err := New(f, 0, 1024*1024*2, false)
	if err != nil {
		b.Fatalf("Failed to create LogFile: %v", err)
	}

	payload := bytes.Repeat([]byte("BenchData"), 100) // 900 bytes

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := w.Write(context.Background(), payload)
			if err != nil {
				b.Fatalf("Write failed: %v", err)
			}
		}
	})
}
