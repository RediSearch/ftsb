package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/RediSearch/ftsb/benchmark_runner"
)

// Program option vars:
var (
	host           string
	password       string
	debug          int
	loader         *benchmark_runner.BenchmarkRunner
	pipeline       int
	clusterMode    bool
	continueOnErr  bool
	timeout        time.Duration
	versionFlag    bool
	logFile        string
	timeoutSeconds int
)

// Parse args:
func init() {
	loader = benchmark_runner.GetBenchmarkRunnerWithBatchSize(100)
	flag.StringVar(&host, "host", "localhost:6379", "The host:port for Redis connection")
	flag.StringVar(&password, "a", "", "Password for Redis Auth.")
	flag.IntVar(&debug, "debug", 0, "Debug printing (choices: 0, 1, 2). (default 0)")
	flag.BoolVar(&continueOnErr, "continue-on-error", true, "If set to true, it will continue the benchmark and print the error message to stderr.")
	flag.BoolVar(&clusterMode, "cluster-mode", false, "If set to true, it will run the client in cluster mode.")
	flag.IntVar(&pipeline, "pipeline", 1, "Pipeline <numreq> requests. Default 1 (no pipeline).")
	flag.IntVar(&timeoutSeconds, "timeout", 60, "Redis connection timeout in seconds.")
	flag.BoolVar(&versionFlag, "version", false, "Print the version and exit.")
	flag.StringVar(&logFile, "log-file", "", "File to write all log output (in addition to stdout/stderr). If not set, logs only to stdout/stderr.")
}

// parseFlags parses the command line after all flags are declared. Parsing
// happens in main (not init) so `go test` can run this package — the test
// harness passes -test.* flags that a parse-at-init would reject.
func parseFlags() {
	flag.Parse()
	// Convert seconds to time.Duration
	timeout = time.Duration(timeoutSeconds) * time.Second

	// Handle version flag
	if versionFlag {
		fmt.Printf("Version: %s (Dirty: %s)\n", GitSHA1, GitDirty)
		os.Exit(0)
	}
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
	configs["logFile"] = logFile
	return configs
}

type RedisIndexer struct {
	partitions uint
}

func (i *RedisIndexer) GetIndex(itemsRead uint64, p *benchmark_runner.DocHolder) int {
	return int(uint(itemsRead) % i.partitions)
}

func (b *benchmark) GetCmdDecoder(br *bufio.Reader, maxTokenSizeMB uint) benchmark_runner.DocDecoder {
	scanner := bufio.NewScanner(br)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, int(maxTokenSizeMB*1024*1024))
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
	parseFlags()
	b := benchmark{}
	git_sha := toolGitSHA1()
	git_dirty_str := ""
	if toolGitDirty() {
		git_dirty_str = "-dirty"
	}

	// Route console logging through a non-blocking writer so a stalled output
	// consumer (wedged terminal, run-remote/SSH stream stall, full CI buffer)
	// can never wedge the benchmark and prevent the result from being written
	// (issue #121). The log file, if any, is written directly -- a regular file
	// does not stall, and keeping it off the drop path leaves it complete.
	console := newNonBlockingWriter(os.Stderr, 1024)
	if logFile != "" {
		f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			log.Fatalf("Failed to open log file %s: %v", logFile, err)
		}
		defer f.Close()
		log.SetOutput(io.MultiWriter(console, f))
		log.Printf("Logging to file: %s\n", logFile)
	} else {
		log.SetOutput(console)
	}

	log.Printf("ftsb (git_sha1:%s%s)\n", git_sha, git_dirty_str)
	loader.RunBenchmark(&b, benchmark_runner.SingleQueue)
}
