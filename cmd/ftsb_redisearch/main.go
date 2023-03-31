package main

import (
	"bufio"
	"flag"
	"github.com/RediSearch/ftsb/benchmark_runner"
	"log"
)

// Program option vars:
var (
	host          string
	password      string
	debug         int
	loader        *benchmark_runner.BenchmarkRunner
	pipeline      int
	clusterMode   bool
	continueOnErr bool
)

// Parse args:
func init() {
	loader = benchmark_runner.GetBenchmarkRunnerWithBatchSize(10)
	flag.StringVar(&host, "host", "localhost:6379", "The host:port for Redis connection")
	flag.StringVar(&password, "a", "", "Password for Redis Auth.")
	flag.IntVar(&debug, "debug", 0, "Debug printing (choices: 0, 1, 2). (default 0)")
	flag.BoolVar(&continueOnErr, "continue-on-error", false, "If set to true, it will continue the benchmark and print the error message to stderr.")
	flag.BoolVar(&clusterMode, "cluster-mode", false, "If set to true, it will run the client in cluster mode.")
	flag.IntVar(&pipeline, "pipeline", 1, "Pipeline <numreq> requests. Default 1 (no pipeline).")
	flag.Parse()
}

type benchmark struct {
}

func (b *benchmark) GetConfigurationParametersMap() map[string]interface{} {
	configs := map[string]interface{}{}
	configs["host"] = host
	configs["clusterMode"] = clusterMode
	configs["continueOnError"] = continueOnErr
	configs["debug"] = debug
	configs["pipeline"] = pipeline
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
	b := benchmark{}
	git_sha := toolGitSHA1()
	git_dirty_str := ""
	if toolGitDirty() {
		git_dirty_str = "-dirty"
	}
	log.Printf("ftsb (git_sha1:%s%s)\n", git_sha, git_dirty_str)
	loader.RunBenchmark(&b, benchmark_runner.SingleQueue)
}
