package main

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alialaee/logfile"
	"github.com/cockroachdb/pebble/record"
	"github.com/hashicorp/raft"
	hashicorp "github.com/hashicorp/raft-wal"
	tidwall "github.com/tidwall/wal"
)

var randSrc = rand.NewSource(100) // Fixed seed for reproducibility
var rng = rand.New(randSrc)

func main() {
	totalBytes, records := createRecords(512, 1024*1024, 50)

	concurrency := 20
	measureAndPrintResults("Logfile Write", totalBytes*int64(concurrency), func() error {
		return benchmarkLogfileWrite(records, concurrency)
	})

	repeat := 5
	measureAndPrintResults("Direct File Write+Flush", totalBytes*int64(repeat), func() error {
		return benchmarkWriteFlush(records, repeat)
	})

	repeat = 5
	measureAndPrintResults("Tidwall WAL Write", totalBytes*int64(repeat), func() error {
		return benchmarkTidwallWrite(records, repeat)
	})

	repeat = 5
	measureAndPrintResults("Hashicorp Raft Write", totalBytes*int64(repeat), func() error {
		return benchmarkHashicorpWrite(records, repeat)
	})

	concurrency = 20
	measureAndPrintResults("Pebble Record Write", totalBytes*int64(concurrency), func() error {
		return benchmarkPebbleWrite(records, concurrency)
	})
}

func measureAndPrintResults(name string, totalBytes int64, fn func() error) {
	startTime := time.Now()
	if err := fn(); err != nil {
		panic(err)
	}
	timeTaken := time.Since(startTime)
	fmt.Printf("=== %s ===\n", name)
	fmt.Printf("Total bytes written: %d MB\n", totalBytes/1024/1024)
	fmt.Printf("Time taken: %s\n", timeTaken.String())
	fmt.Printf("Throughput: %.2f MB/s\n", float64(totalBytes)/timeTaken.Seconds()/1024/1024)
	fmt.Println()
}

func benchmarkWriteFlush(records [][]byte, concurrency int) error {
	tmpFile, err := os.CreateTemp("", "logfilebenchmark")
	if err != nil {
		panic(err)
	}
	defer func() { _ = tmpFile.Close(); _ = os.Remove(tmpFile.Name()) }()

	for range concurrency {
		for _, rec := range records {
			if _, err := tmpFile.Write(rec); err != nil {
				return err
			}

			if err := tmpFile.Sync(); err != nil {
				return err
			}
		}
	}
	return nil
}

func benchmarkLogfileWrite(records [][]byte, concurrency int) error {
	tmpFile, err := os.CreateTemp("", "logfilebenchmark")
	if err != nil {
		panic(err)
	}
	defer func() { _ = tmpFile.Close(); _ = os.Remove(tmpFile.Name()) }()

	lf, err := logfile.New(tmpFile, 0, 8*1024*1024, false)
	if err != nil {
		return err
	}
	defer func() { _ = lf.Close() }()

	wg := &sync.WaitGroup{}
	for range concurrency {
		wg.Go(func() {
			for _, rec := range records {
				if _, err := lf.Write(context.Background(), rec); err != nil {
					panic(err)
				}
			}
		})
	}

	wg.Wait()
	return nil
}

func benchmarkTidwallWrite(records [][]byte, concurrency int) error {
	tempDir, err := os.MkdirTemp("", "tidwallbenchmark")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	log, err := tidwall.Open(tempDir, nil)
	if err != nil {
		return err
	}

	// wg := &sync.WaitGroup{}
	var index atomic.Int64
	for range concurrency {
		// wg.Go(func() {
		for _, rec := range records {
			i := index.Add(1)
			if err := log.Write(uint64(i), rec); err != nil {
				panic(err)
			}
		}
		// })
	}
	// wg.Wait()

	defer func() { _ = log.Close() }()
	return nil
}

func benchmarkHashicorpWrite(records [][]byte, repeat int) error {
	tempDir, err := os.MkdirTemp("", "hashicorpbenchmark")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	log, err := hashicorp.Open(tempDir)
	if err != nil {
		return err
	}

	var index atomic.Int64
	for range repeat {
		for _, rec := range records {
			err := log.StoreLog(&raft.Log{
				Index: uint64(index.Add(1)),
				Data:  rec,
			})
			if err != nil {
				panic(err)
			}
		}
	}

	return nil
}

func benchmarkPebbleWrite(records [][]byte, concurrency int) error {
	tmpFile, err := os.CreateTemp("", "logfilebenchmark")
	if err != nil {
		panic(err)
	}
	fileName := tmpFile.Name()
	defer func() { _ = os.Remove(fileName) }()

	log := record.NewLogWriter(tmpFile, 1, record.LogWriterConfig{})
	defer func() { _ = log.Close() }()

	wg := &sync.WaitGroup{}
	var mu sync.Mutex
	for range concurrency {
		wg.Go(func() {
			wg2 := &sync.WaitGroup{}
			var errOut error
			for _, rec := range records {
				mu.Lock()
				wg2.Add(1)
				_, err := log.SyncRecord(rec, wg2, &errOut)
				mu.Unlock()
				if err != nil {
					panic(err)
				}
				wg2.Wait()

				if errOut != nil {
					panic(errOut)
				}
			}
		})
	}
	wg.Wait()

	return nil
}

func createRecords(minSize int, maxSize int, count int) (totalBytes int64, records [][]byte) {
	for range count {
		randSize := minSize + rng.Intn(maxSize-minSize+1)
		buf := make([]byte, randSize)
		if n, err := crand.Read(buf); err != nil && n != randSize {
			panic(err)
		}
		records = append(records, buf)
		totalBytes += int64(randSize)
	}
	return totalBytes, records
}
