package common

import (
	"github.com/RediSearch/redisearch-go/redisearch"
)

// SimulatorConfig is an interface to create a Simulator
type SimulatorConfig interface {
	NewSimulator(uint64, string, int, []string, int64) Simulator
}

// Simulator simulates a use case.
type Simulator interface {
	Finished() bool
	Next(document *redisearch.Document) bool
}
