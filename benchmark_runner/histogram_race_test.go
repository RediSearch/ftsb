package benchmark_runner

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

func newTestRunner() *BenchmarkRunner {
	l := &BenchmarkRunner{maxLatencySeconds: 1}
	l.initHistograms()
	l.detailedMapHistograms = make(map[string]*hdrhistogram.Histogram)
	l.perSecondHistograms = make(map[uint64]*hdrhistogram.Histogram)
	return l
}

// Regression guard for issue #116: recordCmdStat is called from every worker
// goroutine and races the reporter's reads/reset of the same (non-concurrency-
// safe) hdr histograms. With histogramsMutex this must be race-free and lose no
// increments. Run under -race (see `make unit-test` / the benchmark_runner CI
// step) to actually detect a regression; the totalOps check catches lost writes
// even without -race.
func TestRecordCmdStatConcurrentIsRaceFree(t *testing.T) {
	l := newTestRunner()
	labels := []string{"WRITE", "READ", "UPDATE", "DELETE", "SETUP_WRITE", "READ_CURSOR"}
	const workers, perWorker = 8, 2000

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				cs := NewCmdStat([]byte(labels[i%len(labels)]), []byte("q1"), uint64(i%1000+1), false, false, 10, 20)
				l.recordCmdStat(*cs)
			}
		}()
	}
	// A reporter-style reader concurrent with the writers, mirroring report()'s
	// access pattern (reads + inst reset) under the same lock.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			l.histogramsMutex.Lock()
			_ = l.totalHistogram.TotalCount()
			_ = l.writeHistogram.ValueAtQuantile(50.0)
			l.inst_writeHistogram.Reset()
			l.histogramsMutex.Unlock()
		}
	}()
	wg.Wait()

	if got := atomic.LoadUint64(&l.totalOps); got != uint64(workers*perWorker) {
		t.Fatalf("totalOps = %d, want %d (no increments must be lost)", got, workers*perWorker)
	}
}

// The periodic reporter must stop promptly when stopReport is closed and signal
// via reportDone, so the final read-out never runs concurrently with it (the
// root cause of the leaked-goroutine timeseries race).
func TestReportStopsCleanly(t *testing.T) {
	l := newTestRunner()
	l.stopReport = make(chan struct{})
	l.reportDone = make(chan struct{})

	go l.report(5*time.Millisecond, time.Now())
	time.Sleep(20 * time.Millisecond) // allow a few ticks
	close(l.stopReport)

	select {
	case <-l.reportDone:
		// stopped cleanly
	case <-time.After(2 * time.Second):
		t.Fatal("report() did not exit within 2s of closing stopReport (goroutine leak)")
	}
}
