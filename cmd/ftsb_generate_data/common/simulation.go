package common

import (
	"io"
	"time"

	"github.com/filipecosta90/ftsb/cmd/ftsb_generate_data/serialize"
)

// SimulatorConfig is an interface to create a Simulator from a time.Duration
type SimulatorConfig interface {
	NewSimulator(uint64, string, string) Simulator
}

// Simulator simulates a use case.
type Simulator interface {
	Finished() bool
	Next(*serialize.Document) bool
	CreateIdx(Idx string, writer io.Writer)
	//Fields() map[string][][]byte
}

// SimulatedMeasurement simulates one measurement (e.g. Redis for DevOps).
type SimulatedMeasurement interface {
	Tick(time.Duration)
	ToDocument(*serialize.Document)
}
