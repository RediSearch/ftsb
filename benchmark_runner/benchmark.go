package benchmark_runner

import "bufio"

// Benchmark is an interface that represents the skeleton of a program
// needed to run an insert or benchmark benchmark.
type Benchmark interface {
	// GetCmdDecoder returns the DocDecoder to use for this Benchmark
	GetCmdDecoder(br *bufio.Reader) DocDecoder

	// GetBatchFactory returns the BatchFactory to use for this Benchmark
	GetBatchFactory() BatchFactory

	// GetCommandIndexer returns the DocIndexer to use for this Benchmark
	GetCommandIndexer(maxPartitions uint) DocIndexer

	// GetProcessor returns the Processor to use for this Benchmark
	GetProcessor() Processor

	// GetConfigurationParametersMap returns the map of specific configurations used in the benchmark
	GetConfigurationParametersMap() map[string]interface{}
}
