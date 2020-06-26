package main

import (
	"bufio"
	"flag"
	"github.com/RediSearch/ftsb/benchmark_runner"
)

// Program option vars:
var (
	host                    string
	debug                   int
	loader                  *benchmark_runner.BenchmarkRunner
	PoolPipelineConcurrency int
	PoolPipelineWindow      float64
	clusterMode             bool
	singleWorkerQueue       bool
)

// Parse args:
func init() {
	loader = benchmark_runner.GetBenchmarkRunnerWithBatchSize(10)
	flag.StringVar(&host, "host", "localhost:6379", "The host:port for Redis connection")
	flag.IntVar(&debug, "debug", 0, "Debug printing (choices: 0, 1, 2). (default 0)")
	flag.BoolVar(&clusterMode, "cluster-mode", false, "If set to true, it will run the client in cluster mode.")
	flag.Float64Var(&PoolPipelineWindow, "pipeline-window-ms", 0.5, "If window is zero then implicit pipelining will be disabled")
	flag.IntVar(&PoolPipelineConcurrency, "pipeline-max-size", 100, "If limit is zero then no limit will be used and pipelines will only be limited by the specified time window")
	flag.BoolVar(&singleWorkerQueue, "workers-single-queue", true, "If set to true, it will use a single shared queue across all workers.")
	flag.Parse()
}

type benchmark struct {
}

func (b *benchmark) GetConfigurationParametersMap() map[string]interface{} {
	configs := map[string]interface{}{}
	configs["host"] = host
	configs["clusterMode"] = clusterMode
	configs["singleWorkerQueue"] = singleWorkerQueue
	configs["debug"] = debug
	configs["PoolPipelineWindow"] = PoolPipelineWindow
	configs["PoolPipelineConcurrency"] = PoolPipelineConcurrency
	return configs
}

type RedisIndexer struct {
	partitions uint
}

func (i *RedisIndexer) GetIndex(itemsRead uint64, p *benchmark_runner.DocHolder) int {
	return int(uint(itemsRead) % i.partitions)
}

func (b *benchmark) GetCmdDecoder(br *bufio.Reader) benchmark_runner.DocDecoder {
	scanner := bufio.NewScanner(br)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	return &decoder{scanner: scanner}
}

func (b *benchmark) GetBatchFactory() benchmark_runner.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetCommandIndexer(maxPartitions uint) benchmark_runner.DocIndexer {
	return &RedisIndexer{partitions: maxPartitions}
}

func (b *benchmark) GetProcessor() benchmark_runner.Processor {
	return &processor{}
}

func main() {
	//if singleWorkerQueue {
	b := benchmark{}
	loader.RunBenchmark(&b, benchmark_runner.SingleQueue)
	//} else {
	//	loader.RunBenchmark(&benchmark{dbc: &creator}, load.WorkerPerQueue)
	//}
}
