package main

import (
	"io"
	"time"
)

// nonBlockingWriter forwards whole log lines to an underlying writer on a
// dedicated drain goroutine, so a stalled consumer can never wedge the caller.
//
// ftsb's progress reporter logs unboundedly (one line per --reporting-period)
// and summary() logs before it writes the --json-out-file result. If stdout/
// stderr is a pipe whose consumer stops draining (a wedged terminal, an SSH /
// run-remote stream stall, a full CI log buffer), a plain blocking log.Printf
// wedges the whole run: the process never exits and the result is never written
// (issue #121). os.Stderr is in blocking mode, so its write can't be bounded
// with a deadline; instead the blocking write happens on the drain goroutine and
// Write only waits for delivery up to writeTimeout.
//
// On a HEALTHY consumer the drain goroutine writes each line immediately, so
// Write returns only after the line has actually reached the underlying writer.
// That matters for messages logged right before the process exits -- notably the
// final summary and any log.Fatal message, which is emitted via log.Output and
// then os.Exit(1), bypassing any deferred flush. Because Write is synchronous on
// a healthy consumer, those lines are delivered before the exit.
//
// On a STALLED consumer Write waits at most writeTimeout and then returns
// (dropping delivery of that line) so the benchmark still completes and writes
// its result; once the small buffer fills, further lines are dropped without
// waiting at all.
type nonBlockingWriter struct {
	ch           chan writeReq
	writeTimeout time.Duration
}

type writeReq struct {
	b    []byte
	done chan struct{} // closed by the drain goroutine once b has been written
}

// newNonBlockingWriter starts a drain goroutine that forwards buffered lines to
// w. bufLines is how many pending lines are tolerated before a stalled consumer
// causes new lines to be dropped; writeTimeout bounds how long a single Write
// waits for its line to be delivered before giving up.
func newNonBlockingWriter(w io.Writer, bufLines int, writeTimeout time.Duration) *nonBlockingWriter {
	nb := &nonBlockingWriter{
		ch:           make(chan writeReq, bufLines),
		writeTimeout: writeTimeout,
	}
	go func() {
		for req := range nb.ch {
			// A stalled consumer blocks here; that's fine -- only this detached
			// goroutine waits, never the benchmark. Write errors are ignored,
			// matching the standard log package's best-effort semantics.
			_, _ = w.Write(req.b)
			close(req.done)
		}
	}()
	return nb
}

// Write copies the line (the log package reuses its formatting buffer), hands it
// to the drain goroutine, and waits up to writeTimeout for it to be written. It
// never blocks longer than writeTimeout, and never loses a partial line -- the
// log package calls Write once per fully-formatted line, so a dropped line is
// dropped whole.
func (nb *nonBlockingWriter) Write(p []byte) (int, error) {
	b := make([]byte, len(p))
	copy(b, p)
	req := writeReq{b: b, done: make(chan struct{})}
	select {
	case nb.ch <- req:
		// Delivered to the drain goroutine; wait (bounded) for it to be written
		// so healthy-consumer output -- including a pre-os.Exit fatal line -- is
		// actually flushed before we return.
		select {
		case <-req.done:
		case <-time.After(nb.writeTimeout):
		}
	default:
		// Buffer full: the consumer is stalled and we're already behind. Drop
		// this line rather than wait, so the benchmark keeps making progress.
	}
	return len(p), nil
}
