package main

import (
	"io"
	"sync"
	"time"
)

// nonBlockingWriter forwards whole log lines to an underlying writer through a
// bounded buffer, dropping output rather than blocking when the consumer stalls.
//
// ftsb's progress reporter logs unboundedly (one line per --reporting-period)
// and summary() logs before it writes the --json-out-file result. If stdout/
// stderr is a pipe whose consumer stops draining (a wedged terminal, an SSH /
// run-remote stream stall, a full CI log buffer), a plain blocking log.Printf
// wedges the whole run: the process never exits and the result is never written
// (issue #121). Routing console logs through this writer makes every log.Printf
// non-blocking, so the benchmark always completes, writes its result, and exits.
//
// On a HEALTHY consumer nothing is lost: the drain goroutine keeps up, so the
// buffer never fills, and Close() flushes the tail (including the final summary)
// before the process exits. On a STALLED consumer Close() still returns within
// its timeout, so shutdown can never hang.
type nonBlockingWriter struct {
	ch   chan []byte
	done chan struct{} // closed when the drain goroutine has finished

	mu     sync.Mutex // guards closed + the send on ch; never held across w.Write
	closed bool
}

// newNonBlockingWriter starts a drain goroutine that forwards buffered lines to
// w. bufLines is the number of pending lines tolerated before new lines are
// dropped.
func newNonBlockingWriter(w io.Writer, bufLines int) *nonBlockingWriter {
	nb := &nonBlockingWriter{
		ch:   make(chan []byte, bufLines),
		done: make(chan struct{}),
	}
	go func() {
		defer close(nb.done)
		for b := range nb.ch {
			// A stalled consumer blocks here; that's fine -- only this detached
			// goroutine waits, never the benchmark. Write errors are ignored,
			// matching the standard log package's best-effort semantics.
			_, _ = w.Write(b)
		}
	}()
	return nb
}

// Write never blocks. The log package calls Write once per fully-formatted line,
// so a dropped write loses a whole line, never a partial one. The line is copied
// because log reuses its formatting buffer across calls.
func (nb *nonBlockingWriter) Write(p []byte) (int, error) {
	b := make([]byte, len(p))
	copy(b, p)
	nb.mu.Lock()
	if !nb.closed {
		select {
		case nb.ch <- b:
		default: // buffer full (consumer stalled) -> drop this line
		}
	}
	nb.mu.Unlock()
	return len(p), nil
}

// Close stops accepting new lines and waits up to timeout for the drain
// goroutine to flush whatever is still buffered. On a healthy consumer this
// delivers the final lines (notably the summary) before the process exits; on a
// stalled consumer it returns after timeout so shutdown never hangs (#121).
// Safe to call more than once; writes after Close are silently dropped.
func (nb *nonBlockingWriter) Close(timeout time.Duration) {
	nb.mu.Lock()
	if nb.closed {
		nb.mu.Unlock()
		return
	}
	nb.closed = true
	close(nb.ch)
	nb.mu.Unlock()

	select {
	case <-nb.done:
	case <-time.After(timeout):
	}
}
