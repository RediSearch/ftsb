package benchmark_runner

import "testing"

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
