package main

import "io"

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
// The background drain goroutine may block on the stalled consumer, but it is
// detached and touches no benchmark state, so it is reaped harmlessly at exit.
type nonBlockingWriter struct {
	ch chan []byte
}

// newNonBlockingWriter starts a drain goroutine that forwards buffered lines to
// w. bufLines is the number of pending lines tolerated before new lines are
// dropped.
func newNonBlockingWriter(w io.Writer, bufLines int) *nonBlockingWriter {
	nb := &nonBlockingWriter{ch: make(chan []byte, bufLines)}
	go func() {
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
	select {
	case nb.ch <- b:
	default: // buffer full (consumer stalled) -> drop this line
	}
	return len(p), nil
}
