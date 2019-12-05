package common

import (
	"github.com/RediSearch/redisearch-go/redisearch"
)

// SimulatorConfig is an interface to create a Simulator
type SimulatorConfig interface {
	NewSimulator(limit uint64, inputFilename string, debug int, stopwords []string, seed int64) Simulator
	NewSyntheticsSimulator(limit uint64, debug int, stopwords []string, numberFields uint64, maxCardinalityPerField uint64, seed int64) Simulator
}

// Simulator simulates a use case.
type Simulator interface {
	Finished() bool
	Next(document *redisearch.Document) bool
}
