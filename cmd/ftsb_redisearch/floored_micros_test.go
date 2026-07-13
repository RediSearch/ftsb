package main

import (
	"testing"
	"time"
)

// Deterministically guards the 1us latency floor (independent of wall-clock
// timing, unlike the fake-client pipeline test): a sub-microsecond duration
// must never record a physically-impossible 0us network latency.
func TestFlooredMicros(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want uint64
	}{
		{0, 1},                      // exactly zero -> floored
		{500 * time.Nanosecond, 1},  // 0.5us truncates to 0 -> floored
		{1500 * time.Nanosecond, 1}, // 1.5us truncates to 1
		{2 * time.Microsecond, 2},
		{1234 * time.Microsecond, 1234},
	}
	for _, c := range cases {
		if got := flooredMicros(c.d); got != c.want {
			t.Errorf("flooredMicros(%v) = %d, want %d", c.d, got, c.want)
		}
	}
}
