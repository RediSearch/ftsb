package main

import (
	"bufio"
	"flag"
	"github.com/RediSearch/ftsb/load"
)

// Program option vars:
var (
	host                    string
	debug                   int
	loader                  *load.BenchmarkRunner
	PoolPipelineConcurrency int
	PoolPipelineWindow      float64
	clusterMode             bool
	singleWorkerQueue       bool
)

// Parse args:
func init() {
	loader = load.GetBenchmarkRunnerWithBatchSize(10)
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

func (i *RedisIndexer) GetIndex(itemsRead uint64, p *load.DocHolder) int {
	return int(uint(itemsRead) % i.partitions)
}

func (b *benchmark) GetCmdDecoder(br *bufio.Reader) load.DocDecoder {
	return &decoder{scanner: bufio.NewScanner(br)}
}

func (b *benchmark) GetBatchFactory() load.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetCommandIndexer(maxPartitions uint) load.DocIndexer {
	return &RedisIndexer{partitions: maxPartitions}
}

func (b *benchmark) GetProcessor() load.Processor {
	return &processor{}
}

func main() {
	//if singleWorkerQueue {
	b := benchmark{}
	loader.RunBenchmark(&b, load.SingleQueue)
	//} else {
	//	loader.RunBenchmark(&benchmark{dbc: &creator}, load.WorkerPerQueue)
	//}
}
