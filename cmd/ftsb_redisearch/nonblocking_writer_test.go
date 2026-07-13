package main

import (
	"bytes"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// blockingWriter blocks every Write until released -- models a stalled console.
type blockingWriter struct{ release chan struct{} }

func (b *blockingWriter) Write(p []byte) (int, error) { <-b.release; return len(p), nil }

// The whole point of #121: Write must never block the caller for longer than the
// write timeout, even when the underlying consumer is wedged.
func TestNonBlockingWriterDoesNotBlockOnStall(t *testing.T) {
	bw := &blockingWriter{release: make(chan struct{})}
	nb := newNonBlockingWriter(bw, 4, 50*time.Millisecond)

	done := make(chan struct{})
	go func() {
		for i := 0; i < 100000; i++ { // far more than the buffer -> excess is dropped
			_, _ = nb.Write([]byte("progress line\n"))
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		close(bw.release)
		t.Fatal("Write blocked while the underlying writer was stalled")
	}
	close(bw.release) // let the drain goroutine exit
}

// countingWriter records how many lines were delivered and their bytes.
type countingWriter struct {
	mu  sync.Mutex
	n   int64
	buf bytes.Buffer
}

func (c *countingWriter) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	atomic.AddInt64(&c.n, 1)
	c.buf.Write(p)
	return len(p), nil
}
func (c *countingWriter) contains(s string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return bytes.Contains(c.buf.Bytes(), []byte(s))
}

// The regression this replaces (#122 CI failure): a message must be on the
// underlying writer by the time Write RETURNS on a healthy consumer, so a line
// logged immediately before os.Exit (log.Fatal, the final summary) is delivered
// even though os.Exit bypasses any deferred flush. Synchronous delivery makes
// this deterministic -- no polling.
func TestNonBlockingWriterDeliversBeforeReturnOnHealthy(t *testing.T) {
	cw := &countingWriter{}
	nb := newNonBlockingWriter(cw, 16, 2*time.Second)

	_, _ = nb.Write([]byte("Fatal error with X\n"))

	if got := atomic.LoadInt64(&cw.n); got != 1 {
		t.Fatalf("delivered %d lines by the time Write returned, want 1", got)
	}
	if !cw.contains("Fatal error with X") {
		t.Fatal("healthy-consumer line was not delivered before Write returned")
	}
}

// When the consumer keeps up, nothing is dropped and every line is delivered by
// the time the write loop finishes -- again deterministic, no polling.
func TestNonBlockingWriterDeliversAllWhenDrained(t *testing.T) {
	cw := &countingWriter{}
	nb := newNonBlockingWriter(cw, 1024, 2*time.Second)
	const lines = 500
	for i := 0; i < lines; i++ {
		_, _ = nb.Write([]byte("x"))
	}
	if got := atomic.LoadInt64(&cw.n); got != lines {
		t.Fatalf("delivered %d lines, want %d (nothing should drop when the consumer keeps up)", got, lines)
	}
}
