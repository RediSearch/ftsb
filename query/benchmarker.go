package query

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime/pprof"
	"sync"
	"time"
)

const (
	labelAllQueries  = "All queries"
	labelColdQueries = "Cold queries"
	labelWarmQueries = "Warm queries"

	defaultReadSize = 4 << 20 // 4 MB
)

// BenchmarkRunner contains the common components for running a query benchmarking
// program against a database.
type BenchmarkRunner struct {
	// flag fields
	dbName         string
	limit          uint64
	memProfile     string
	workers        uint
	printResponses bool
	debug          int
	fileName       string
	outputFileLatencySlowlog string
	latencySlowlog uint64
	outputFileStatsResponseLatencyHist string
	outputFileStatsResponseDocCountHist string

	// non-flag fields
	br      *bufio.Reader
	sp      *statProcessor
	scanner *scanner
	ch      chan Query
}

// NewBenchmarkRunner creates a new instance of BenchmarkRunner which is
// common functionality to be used by query benchmarker programs
func NewBenchmarkRunner() *BenchmarkRunner {
	runner := &BenchmarkRunner{}
	runner.scanner = newScanner(&runner.limit)
	runner.sp = &statProcessor{
		limit: &runner.limit,
	}
	flag.Uint64Var(&runner.sp.burnIn, "burn-in", 0, "Number of queries to ignore before collecting statistics.")
	flag.Uint64Var(&runner.limit, "max-queries", 0, "Limit the number of queries to send, 0 = no limit")
	flag.Uint64Var(&runner.sp.printInterval, "print-interval", 100, "Print timing stats to stderr after this many queries (0 to disable)")
	flag.StringVar(&runner.memProfile, "memprofile", "", "Write a memory profile to this file.")
	flag.UintVar(&runner.workers, "workers", 1, "Number of concurrent requests to make.")
	flag.BoolVar(&runner.sp.prewarmQueries, "prewarm-queries", false, "Run each query twice in a row so the warm query is guaranteed to be a cache hit")
	flag.BoolVar(&runner.printResponses, "print-responses", false, "Pretty print response bodies for correctness checking (default false).")
	flag.IntVar(&runner.debug, "debug", 0, "Whether to print debug messages.")
	flag.StringVar(&runner.fileName, "file", "", "File name to read queries from")
	flag.StringVar(&runner.outputFileLatencySlowlog, "output-file-latency-slowlog", "", "File name to output slow queries to")
	flag.Uint64Var(&runner.latencySlowlog, "latency-slowlog", 500, "Consider slow queries the ones with response latency bigger than this defined value")
	flag.StringVar(&runner.outputFileStatsResponseLatencyHist, "output-file-stats-response-latency-hist", "stats-response-latency-hist.txt", "File name to output the response latency histogram to")
	flag.StringVar(&runner.outputFileStatsResponseDocCountHist, "output-file-stats-response-doccount-hist", "stats-response-doccount-hist.txt", "File name to output the response document count histogram to")

	return runner
}

// SetLimit changes the number of queries to run, with 0 being all of them
func (b *BenchmarkRunner) SetLimit(limit uint64) {
	b.limit = limit
}

// DoPrintResponses indicates whether responses for queries should be printed
func (b *BenchmarkRunner) DoPrintResponses() bool {
	return b.printResponses
}

// DebugLevel returns the level of debug messages for this benchmark
func (b *BenchmarkRunner) DebugLevel() int {
	return b.debug
}

// DatabaseName returns the name of the database to run queries against
func (b *BenchmarkRunner) DatabaseName() string {
	return b.dbName
}

// ProcessorCreate is a function that creates a new Processor (called in Run)
type ProcessorCreate func() Processor

// Processor is an interface that handles the setup of a query processing worker and executes queries one at a time
type Processor interface {

	// Init initializes at global state for the Processor, possibly based on its worker number / ID
	Init(workerNum int, wg *sync.WaitGroup, m chan uint64, rs chan uint64)

	// ProcessQuery handles a given query and reports its stats
	ProcessQuery(q Query, isWarm bool) ([]*Stat, error)
}

// GetBufferedReader returns the buffered Reader that should be used by the loader
func (b *BenchmarkRunner) GetBufferedReader() *bufio.Reader {
	if b.br == nil {
		if len(b.fileName) > 0 {
			// Read from specified file
			file, err := os.Open(b.fileName)
			if err != nil {
				panic(fmt.Sprintf("cannot open file for read %s: %v", b.fileName, err))
			}
			b.br = bufio.NewReaderSize(file, defaultReadSize)
		} else {
			// Read from STDIN
			b.br = bufio.NewReaderSize(os.Stdin, defaultReadSize)
		}
	}
	return b.br
}

// Run does the bulk of the benchmark execution.
// It launches a gorountine to track stats, creates workers to process queries,
// read in the input, execute the queries, and then does cleanup.
func (b *BenchmarkRunner) Run(queryPool *sync.Pool, processorCreateFn ProcessorCreate) {
	if b.workers == 0 {
		panic("must have at least one worker")
	}
	if b.sp.burnIn > b.limit {
		panic("burn-in is larger than limit")
	}
	b.ch = make(chan Query, b.workers)

	// Launch the stats processor:
	go b.sp.process(b.workers, b.outputFileStatsResponseLatencyHist, b.outputFileStatsResponseDocCountHist )

	// Launch query processors
	var wg sync.WaitGroup
	for i := 0; i < int(b.workers); i++ {
		wg.Add(1)
		go b.processorHandler(&wg, queryPool, processorCreateFn(), i)
	}

	// Read in jobs, closing the job channel when done:
	// Wall clock start time
	wallStart := time.Now()
	br := b.scanner.setReader(b.GetBufferedReader())
	_ = br.scan(queryPool, b.ch)
	close(b.ch)

	// Block for workers to finish sending requests, closing the stats channel when done:
	wg.Wait()
	b.sp.CloseAndWait()


	// Wall clock end time
	wallEnd := time.Now()
	wallTook := wallEnd.Sub(wallStart)
	_, err := fmt.Printf("Took: %8.3f sec\n", float64(wallTook.Nanoseconds())/1e9)
	if err != nil {
		log.Fatal(err)
	}

	_, _ = fmt.Printf("Saving Debug Info with query and doc count to %s\n", "debug-query-doc-count.txt" )

	d0 := []byte(b.sp.StatsMapping[labelAllQueries].StringDocCountDebug())
	fErr := ioutil.WriteFile("debug-query-doc-count.txt", d0, 0644)
	if fErr != nil {
		log.Fatal(err)
	}

	_, _ = fmt.Printf("Saving Query Latencies Full Histogram to %s\n",b.outputFileStatsResponseLatencyHist )

	d1 := []byte(b.sp.StatsMapping[labelAllQueries].stringQueryLatencyFullHistogram())
	fErr = ioutil.WriteFile(b.outputFileStatsResponseLatencyHist, d1, 0644)
	if fErr != nil {
		log.Fatal(err)
	}
	_, _ = fmt.Printf("Saving Query response Document Count Full Histogram to %s\n",b.outputFileStatsResponseDocCountHist )

	d2 := []byte(b.sp.StatsMapping[labelAllQueries].stringQueryResponseSizeFullHistogram())
	fErr = ioutil.WriteFile(b.outputFileStatsResponseDocCountHist, d2, 0644)
	if fErr != nil {
		log.Fatal(err)
	}

	// (Optional) create a memory profile:
	if len(b.memProfile) > 0 {
		f, err := os.Create(b.memProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
	}
}

func (b *BenchmarkRunner) processorHandler(wg *sync.WaitGroup, queryPool *sync.Pool, processor Processor, workerNum int) {
	buflen := uint64(len(b.ch))
	metricsChan := make(chan uint64, buflen)
	pwg := &sync.WaitGroup{}
	responseSizesChan := make(chan uint64, buflen)
	pwg.Add(1)

	processor.Init(workerNum, pwg, metricsChan, responseSizesChan)

	for query := range b.ch {
		stats, err := processor.ProcessQuery(query, false)
		if err != nil {
			panic(err)
		}
		b.sp.sendStats(stats)

		// If PrewarmQueries is set, we run the query as 'cold' first (see above),
		// then we immediately run it a second time and report that as the 'warm' stat.
		// This guarantees that the warm stat will reflect optimal cache performance.
		if b.sp.prewarmQueries {
			// Warm run
			stats, err = processor.ProcessQuery(query, true)
			if err != nil {
				panic(err)
			}
			b.sp.sendStatsWarm(stats)
		}
		queryPool.Put(query)
	}

	//pwg.Wait()
	close(metricsChan)
	close(responseSizesChan)

	wg.Done()
}
