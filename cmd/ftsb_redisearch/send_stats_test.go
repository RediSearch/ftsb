package main

import (
	"errors"
	"testing"

	"github.com/RediSearch/ftsb/benchmark_runner"
	radix "github.com/mediocregopher/radix/v3"
)

// fakeClient is a radix.Client whose Do returns a canned error (nil = success).
// It does not populate command receivers, so the recording path in
// sendFlatCmd/sendIfRequired can be exercised without a real Redis (received
// bytes are therefore 0 in these unit tests; reply capture is covered E2E).
type fakeClient struct {
	calls int
	err   error
}

func (f *fakeClient) Do(a radix.Action) error { f.calls++; return f.err }
func (f *fakeClient) Close() error            { return nil }

// Regression guard for issue #111: the bytes we SEND to Redis (txBytesCount)
// must be recorded as Tx(), not Rx().
func TestSendFlatCmdRecordsSentBytesAsTx(t *testing.T) {
	// pipeline=1 forces sendIfRequired to flush on the first command. Set it
	// explicitly (rather than trusting the flag default) so a stray global
	// left by another test can't make the <-p.cmdChan receive block forever.
	savedPipeline := pipeline
	pipeline = 1
	defer func() { pipeline = savedPipeline }()

	p := &processor{cmdChan: make(chan benchmark_runner.Stat, 1)}
	const txBytesCount = uint64(4096) // request/sent bytes for this command

	_, hadError := sendFlatCmd(
		p, &fakeClient{}, "WRITE", "w1", "HSET",
		[]string{"doc:1", "vec", "payload"}, txBytesCount, nil,
	)
	if hadError {
		t.Fatal("unexpected error from fake client")
	}

	stat := <-p.cmdChan
	entries := stat.CmdStats()
	if len(entries) != 1 {
		t.Fatalf("expected 1 stat entry, got %d", len(entries))
	}
	if got := entries[0].Tx(); got != txBytesCount {
		t.Fatalf("Tx() = %d, want %d (sent bytes must land in Tx, not Rx)", got, txBytesCount)
	}
	// The fake client never populates the receiver, so received bytes are 0.
	if got := entries[0].Rx(); got != 0 {
		t.Fatalf("Rx() = %d, want 0 (fake client does not populate replies)", got)
	}
}

func TestGetRxLen(t *testing.T) {
	if got := getRxLen("abc"); got != 3 {
		t.Fatalf("getRxLen(string) = %d, want 3", got)
	}
	if got := getRxLen([]byte("abcd")); got != 4 {
		t.Fatalf("getRxLen([]byte) = %d, want 4", got)
	}
	if got := getRxLen([]string{"ab", "cde"}); got != 5 {
		t.Fatalf("getRxLen([]string) = %d, want 5", got)
	}
	// Arrays are summed recursively (e.g. an FT.SEARCH reply of mixed elements).
	if got := getRxLen([]interface{}{"ab", []byte("cd"), int64(100)}); got != 2+2+3 {
		t.Fatalf("getRxLen([]interface{}) = %d, want 7", got)
	}
	if got := getRxLen(int64(12345)); got != 5 {
		t.Fatalf("getRxLen(int64) = %d, want 5", got)
	}
	// The production path stores a *interface{} receiver; it must be dereferenced.
	var boxed interface{} = []byte("hello")
	if got := getRxLen(&boxed); got != 5 {
		t.Fatalf("getRxLen(*interface{}) = %d, want 5", got)
	}
	if got := getRxLen(nil); got != 0 {
		t.Fatalf("getRxLen(nil) = %d, want 0", got)
	}
	if got := getRxLen(42); got != 0 {
		t.Fatalf("getRxLen(int) = %d, want 0 (untyped int not a RESP reply type)", got)
	}
}

// On a command error (continue-on-error), a stat must still be recorded, marked
// as an error, and carry the correct sent-byte count in Tx().
func TestSendFlatCmdRecordsErrorStatWithCorrectTx(t *testing.T) {
	savedPipeline, savedContinue := pipeline, continueOnErr
	pipeline, continueOnErr = 1, true
	defer func() { pipeline, continueOnErr = savedPipeline, savedContinue }()

	p := &processor{cmdChan: make(chan benchmark_runner.Stat, 1)}
	const txBytesCount = uint64(128)

	_, hadError := sendFlatCmd(
		p, &fakeClient{err: errors.New("connection refused")}, "WRITE", "w1", "HSET",
		[]string{"doc:1"}, txBytesCount, nil,
	)
	if !hadError {
		t.Fatal("expected hadError=true when client.Do returns an error")
	}

	stat := <-p.cmdChan
	entries := stat.CmdStats()
	if len(entries) != 1 {
		t.Fatalf("expected 1 stat entry, got %d", len(entries))
	}
	if !entries[0].Error() {
		t.Fatal("entry should be marked as an error")
	}
	if got := entries[0].Tx(); got != txBytesCount {
		t.Fatalf("Tx() = %d, want %d (sent bytes recorded even on error)", got, txBytesCount)
	}
}

// An i/o timeout error must set the timedOut flag on the recorded stat.
func TestSendFlatCmdMarksTimeout(t *testing.T) {
	savedPipeline, savedContinue := pipeline, continueOnErr
	pipeline, continueOnErr = 1, true
	defer func() { pipeline, continueOnErr = savedPipeline, savedContinue }()

	p := &processor{cmdChan: make(chan benchmark_runner.Stat, 1)}
	_, hadError := sendFlatCmd(
		p, &fakeClient{err: errors.New("dial tcp 127.0.0.1:6379: i/o timeout")}, "READ", "r1", "FT.SEARCH",
		[]string{"idx"}, 64, nil,
	)
	if !hadError {
		t.Fatal("expected hadError=true on timeout")
	}
	stat := <-p.cmdChan
	entries := stat.CmdStats()
	if len(entries) != 1 {
		t.Fatalf("expected 1 stat entry, got %d", len(entries))
	}
	if !entries[0].TimedOut() {
		t.Fatal("entry should be marked as timed out for an i/o timeout error")
	}
	if !entries[0].Error() {
		t.Fatal("a timeout is also an error")
	}
	if got := entries[0].Tx(); got != 64 {
		t.Fatalf("Tx() = %d, want 64 (sent bytes recorded even on timeout)", got)
	}
}

// Regression guard for issue #113: with --pipeline > 1, buffering must not panic
// (the old code indexed a length-1 replies slice with the flush position), and
// each command in a flush must record its OWN sent-byte count (the old code
// applied the flushing command's single txBytesCount to every entry).
func TestPipelineRecordsPerCommandTxAndDoesNotPanic(t *testing.T) {
	savedPipeline, savedContinue := pipeline, continueOnErr
	pipeline, continueOnErr = 2, true
	defer func() { pipeline, continueOnErr = savedPipeline, savedContinue }()

	p := &processor{cmdChan: make(chan benchmark_runner.Stat, 2)}
	client := &fakeClient{}

	var pending []pendingCmd
	pending, _ = sendFlatCmd(p, client, "WRITE", "w1", "HSET", []string{"doc:1"}, 100, pending)
	if len(pending) != 1 {
		t.Fatalf("with pipeline=2, first command should buffer (len 1), got %d", len(pending))
	}
	select {
	case <-p.cmdChan:
		t.Fatal("no stat should be emitted before the pipeline window is full")
	default:
	}

	pending, _ = sendFlatCmd(p, client, "WRITE", "w2", "HSET", []string{"doc:2"}, 200, pending)
	if len(pending) != 0 {
		t.Fatalf("after flush the buffer should be empty, got %d", len(pending))
	}

	s1 := <-p.cmdChan
	s2 := <-p.cmdChan
	c1 := s1.CmdStats()[0]
	c2 := s2.CmdStats()[0]
	// Order is preserved: first buffered command recorded first.
	if c1.Tx() != 100 || c2.Tx() != 200 {
		t.Fatalf("per-command Tx wrong: got [%d %d], want [100 200] (old code recorded [200 200])", c1.Tx(), c2.Tx())
	}
	// A whole pipeline shares one send->reply round-trip, so per-command latencies
	// are equal; the >=1us floor means a sub-microsecond timer reading never
	// records a physically-impossible 0us network latency.
	if c1.Latency() < 1 || c2.Latency() < 1 {
		t.Fatalf("latency must be floored to >=1us: got [%d %d]", c1.Latency(), c2.Latency())
	}
	if c1.Latency() != c2.Latency() {
		t.Fatalf("pipelined commands share one round-trip; latencies should be equal: %d != %d", c1.Latency(), c2.Latency())
	}
}
