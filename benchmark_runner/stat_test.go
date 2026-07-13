package benchmark_runner

import (
	"sync/atomic"
	"testing"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

// AddEntry's last two params are (rx, tx) in that order. Lock the mapping so a
// received-byte count lands in Rx() and a sent-byte count lands in Tx(); a
// regression here silently mislabels the TxBytes/RxBytes throughput metrics.
func TestAddEntryMapsRxTxInOrder(t *testing.T) {
	const rxBytes = uint64(7)  // bytes received (reply)
	const txBytes = uint64(13) // bytes sent (request)

	s := NewStat().AddEntry([]byte("READ"), []byte("q1"), 1000, 42, false, false, rxBytes, txBytes)

	entries := s.CmdStats()
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if got := entries[0].Rx(); got != rxBytes {
		t.Fatalf("Rx() = %d, want %d (received bytes)", got, rxBytes)
	}
	if got := entries[0].Tx(); got != txBytes {
		t.Fatalf("Tx() = %d, want %d (sent bytes)", got, txBytes)
	}
}

func TestNewCmdStatMapsRxTxInOrder(t *testing.T) {
	c := NewCmdStat([]byte("READ"), []byte("q1"), 42, false, false, 7, 13)
	if c.Rx() != 7 {
		t.Fatalf("Rx() = %d, want 7", c.Rx())
	}
	if c.Tx() != 13 {
		t.Fatalf("Tx() = %d, want 13", c.Tx())
	}
}

// Guards the user-visible mapping in GetTotalsMap: the accumulated sent bytes
// (txTotalBytes) must surface under "TxBytes" and received under "RxBytes".
// This is the JSON the #111 symptom appeared in, and the aggregation label is
// otherwise untested.
func TestGetTotalsMapMapsTxRxCorrectly(t *testing.T) {
	h := func() *hdrhistogram.Histogram { return hdrhistogram.New(1, 1_000_000_000, 3) }
	b := &BenchmarkRunner{
		totalHistogram:      h(),
		setupWriteHistogram: h(),
		writeHistogram:      h(),
		readHistogram:       h(),
		readCursorHistogram: h(),
		updateHistogram:     h(),
		deleteHistogram:     h(),
		txTotalBytes:        13, // bytes sent
		rxTotalBytes:        7,  // bytes received
	}

	configs := b.GetTotalsMap()
	if got := configs["TxBytes"]; got != uint64(13) {
		t.Fatalf("configs[\"TxBytes\"] = %v, want 13 (sent bytes)", got)
	}
	if got := configs["RxBytes"]; got != uint64(7) {
		t.Fatalf("configs[\"RxBytes\"] = %v, want 7 (received bytes)", got)
	}
}

// TotalOps must come from the exact atomic counter, NOT from the HDR histogram,
// which rejects any latency above its trackable cap (e.g. a slow query or a
// multi-second timeout) and would silently undercount ops. This is the fix for
// the op-drop bug the histogram-derived count had at the high tail.
func TestTotalOpsIsAtomicNotHistogramDerived(t *testing.T) {
	h := func() *hdrhistogram.Histogram { return hdrhistogram.New(1, 1_000_000, 3) }
	b := &BenchmarkRunner{
		totalHistogram:      h(),
		setupWriteHistogram: h(),
		writeHistogram:      h(),
		readHistogram:       h(),
		readCursorHistogram: h(),
		updateHistogram:     h(),
		deleteHistogram:     h(),
	}

	// A latency above the trackable cap (1_000_000us) is rejected by the
	// histogram, so a histogram-derived TotalOps would miss it.
	if err := b.totalHistogram.RecordValue(60_000_000); err == nil {
		t.Fatal("expected RecordValue above cap to return an error")
	}
	if hc := b.totalHistogram.TotalCount(); hc != 0 {
		t.Fatalf("histogram TotalCount = %d, want 0 (the value was rejected)", hc)
	}

	// The atomic counter is the source of truth and is unaffected by that rejection.
	atomic.StoreUint64(&b.totalOps, 3)
	if got := b.GetTotalsMap()["TotalOps"]; got != int64(3) {
		t.Fatalf("TotalOps = %v, want 3 (must be the atomic count, not the histogram's 0)", got)
	}
}
