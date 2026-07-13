package main

import (
	"testing"
	"time"

	"github.com/RediSearch/ftsb/benchmark_runner"
	radix "github.com/mediocregopher/radix/v3"
)

// fakeClient is a radix.Client that records nothing and always succeeds, so the
// recording path in sendFlatCmd/sendIfRequired can be exercised without Redis.
type fakeClient struct{ calls int }

func (f *fakeClient) Do(a radix.Action) error { f.calls++; return nil }
func (f *fakeClient) Close() error            { return nil }

// Regression guard for issue #111: the bytes we SEND to Redis (txBytesCount)
// must be recorded as Tx(), not Rx(). Before the fix the AddEntry arguments
// were swapped, so sent bytes were reported under RxBytes and TxBytes was 0.
func TestSendFlatCmdRecordsSentBytesAsTx(t *testing.T) {
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
