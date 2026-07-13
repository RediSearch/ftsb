package main

import (
	"sync/atomic"
	"testing"
	"time"
)

// blockingWriter blocks every Write until released -- models a stalled console.
type blockingWriter struct{ release chan struct{} }

func (b *blockingWriter) Write(p []byte) (int, error) { <-b.release; return len(p), nil }

// The whole point of #121: Write must never block the caller even when the
// underlying consumer is wedged.
func TestNonBlockingWriterNeverBlocks(t *testing.T) {
	bw := &blockingWriter{release: make(chan struct{})}
	nb := newNonBlockingWriter(bw, 4)

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

type countingWriter struct{ n int64 }

func (c *countingWriter) Write(p []byte) (int, error) { atomic.AddInt64(&c.n, 1); return len(p), nil }

// When the consumer keeps up, no lines are dropped and order/content is intact.
func TestNonBlockingWriterDeliversWhenDrained(t *testing.T) {
	cw := &countingWriter{}
	nb := newNonBlockingWriter(cw, 1024)
	const lines = 500
	for i := 0; i < lines; i++ {
		_, _ = nb.Write([]byte("x"))
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&cw.n) == lines {
			return // all delivered
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("delivered %d lines, want %d (nothing should drop when the consumer drains)", atomic.LoadInt64(&cw.n), lines)
}

// Close must flush buffered lines to a healthy consumer before returning -- this
// is what guarantees the final summary line reaches stderr before the process
// exits (the #122 CI regression: "Issued ..." was queued but never flushed).
func TestNonBlockingWriterCloseFlushesBufferedLines(t *testing.T) {
	cw := &countingWriter{}
	nb := newNonBlockingWriter(cw, 1024)
	const lines = 200
	for i := 0; i < lines; i++ {
		_, _ = nb.Write([]byte("x"))
	}
	nb.Close(2 * time.Second)
	if got := atomic.LoadInt64(&cw.n); got != lines {
		t.Fatalf("after Close delivered %d lines, want %d (Close must flush the tail)", got, lines)
	}
}

// Close must return within its timeout even if the consumer is wedged, so
// shutdown can never hang on a stalled output stream.
func TestNonBlockingWriterCloseReturnsDespiteStall(t *testing.T) {
	bw := &blockingWriter{release: make(chan struct{})}
	nb := newNonBlockingWriter(bw, 4)
	for i := 0; i < 100; i++ { // fill + overflow the buffer
		_, _ = nb.Write([]byte("x"))
	}
	done := make(chan struct{})
	go func() { nb.Close(200 * time.Millisecond); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		close(bw.release)
		t.Fatal("Close blocked while the underlying writer was stalled")
	}
	close(bw.release) // let the drain goroutine exit
}

// Writes after Close are dropped, not panicking on a send to a closed channel.
func TestNonBlockingWriterWriteAfterCloseIsSafe(t *testing.T) {
	cw := &countingWriter{}
	nb := newNonBlockingWriter(cw, 16)
	nb.Close(time.Second)
	if _, err := nb.Write([]byte("late")); err != nil {
		t.Fatalf("Write after Close returned error: %v", err)
	}
	nb.Close(time.Second) // idempotent
}
