package benchmark_runner

import "golang.org/x/time/rate"

// Processor is a type that processes the work for a loading worker
type Processor interface {
	// Init does per-worker setup needed before receiving databuild
	Init(workerNum int, doLoad bool, totalWorkers int)
	// ProcessBatch handles a single batch of databuild
	ProcessBatch(b Batch, doLoad bool, rateLimiter *rate.Limiter, useRateLimiter bool) Stat
}

// ProcessorCloser is a Processor that also needs to close or cleanup afterwards
type ProcessorCloser interface {
	Processor
	// Close cleans up after a Processor
	Close(doLoad bool)
}
