package main

import (
	"errors"
	"testing"
	"time"

	"github.com/RediSearch/ftsb/benchmark_runner"
	radix "github.com/mediocregopher/radix/v3"
)

// fakeClient is a radix.Client whose Do returns a canned error (nil = success),
// so the recording path in sendFlatCmd/sendIfRequired can be exercised without
// a real Redis.
type fakeClient struct {
	calls int
	err   error
}

func (f *fakeClient) Do(a radix.Action) error { f.calls++; return f.err }
func (f *fakeClient) Close() error            { return nil }

// Regression guard for issue #111: the bytes we SEND to Redis (txBytesCount)
// must be recorded as Tx(), not Rx(). Before the fix the AddEntry arguments
// were swapped, so sent bytes were reported under RxBytes and TxBytes was 0.
func TestSendFlatCmdRecordsSentBytesAsTx(t *testing.T) {
	// pipeline=1 forces sendIfRequired to flush on the first command. Set it
	// explicitly (rather than trusting the flag default) so a stray global
	// left by another test can't make the <-p.cmdChan receive block forever.
	savedPipeline := pipeline
	pipeline = 1
	defer func() { pipeline = savedPipeline }()

	p := &processor{cmdChan: make(chan benchmark_runner.Stat, 1)}
	const txBytesCount = uint64(4096) // request/sent bytes for this command

	_, _, hadError := sendFlatCmd(
		p, &fakeClient{}, "WRITE", "w1", "HSET",
		[]string{"doc:1", "vec", "payload"}, txBytesCount,
		make([]radix.CmdAction, 0), make([]interface{}, 0), make([]time.Time, 0),
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
	// Replies are discarded (rcv is a nil interface), so received bytes are 0.
	if got := entries[0].Rx(); got != 0 {
		t.Fatalf("Rx() = %d, want 0 (replies are not captured)", got)
	}
}

func TestGetRxLen(t *testing.T) {
	if got := getRxLen("abc"); got != 3 {
		t.Fatalf("getRxLen(string) = %d, want 3", got)
	}
	if got := getRxLen([]string{"ab", "cde"}); got != 5 {
		t.Fatalf("getRxLen([]string) = %d, want 5", got)
	}
	if got := getRxLen(nil); got != 0 {
		t.Fatalf("getRxLen(nil) = %d, want 0", got)
	}
	if got := getRxLen(42); got != 0 {
		t.Fatalf("getRxLen(int) = %d, want 0", got)
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

	_, _, hadError := sendFlatCmd(
		p, &fakeClient{err: errors.New("connection refused")}, "WRITE", "w1", "HSET",
		[]string{"doc:1"}, txBytesCount,
		make([]radix.CmdAction, 0), make([]interface{}, 0), make([]time.Time, 0),
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
	_, _, hadError := sendFlatCmd(
		p, &fakeClient{err: errors.New("dial tcp 127.0.0.1:6379: i/o timeout")}, "READ", "r1", "FT.SEARCH",
		[]string{"idx"}, 64,
		make([]radix.CmdAction, 0), make([]interface{}, 0), make([]time.Time, 0),
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
