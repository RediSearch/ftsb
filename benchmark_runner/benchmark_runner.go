package benchmark_runner

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"golang.org/x/time/rate"
	"io/ioutil"
	"log"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"code.cloudfoundry.org/bytefmt"
	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

const (
	// defaultBatchSize - default size of batches to be inserted
	defaultBatchSize           = 10000
	defaultReadSize            = 4 << 20 // 4 MB
	CurrentResultFormatVersion = "0.1"

	// WorkerPerQueue is the value for assigning each worker its own queue of batches
	WorkerPerQueue = 0
	// SingleQueue is the value for using a single shared queue across all workers
	SingleQueue = 1
	Inf         = rate.Limit(math.MaxFloat64)
)

// BenchmarkRunner is responsible for initializing and storing common
// flags across all database systems and ultimately running a supplied Benchmark
type BenchmarkRunner struct {
	// flag fields
	JsonOutFile     string
	Metadata        string
	batchSize       uint
	workers         uint
	maxRPS          uint64
	limit           uint64
	doLoad          bool
	reportingPeriod time.Duration
	fileName        string
	start           time.Time
	end             time.Time

	// non-flag fields
	br                         *bufio.Reader
	detailedMapHistogramsMutex sync.RWMutex
	detailedMapHistograms      map[string]*hdrhistogram.Histogram
	setupWriteHistogram        *hdrhistogram.Histogram
	inst_setupWriteHistogram   *hdrhistogram.Histogram
	setupWriteTs               []DataPoint

	perSecondHistograms      map[uint64]*hdrhistogram.Histogram
	perSecondHistogramsMutex sync.RWMutex

	writeHistogram      *hdrhistogram.Histogram
	inst_writeHistogram *hdrhistogram.Histogram

	writeTs []DataPoint

	updateHistogram      *hdrhistogram.Histogram
	inst_updateHistogram *hdrhistogram.Histogram
	updateTs             []DataPoint

	readHistogram      *hdrhistogram.Histogram
	inst_readHistogram *hdrhistogram.Histogram
	readTs             []DataPoint

	readCursorHistogram      *hdrhistogram.Histogram
	inst_readCursorHistogram *hdrhistogram.Histogram
	readCursorTs             []DataPoint

	deleteHistogram      *hdrhistogram.Histogram
	inst_deleteHistogram *hdrhistogram.Histogram
	deleteTs             []DataPoint

	totalHistogram      *hdrhistogram.Histogram
	inst_totalHistogram *hdrhistogram.Histogram
	totalTs             []DataPoint

	txTotalBytes uint64
	rxTotalBytes uint64

	testResult TestResult
}

func (b *BenchmarkRunner) GetTotalsMap() map[string]interface{} {
	configs := map[string]interface{}{}
	//TotalOps
	configs["TotalOps"] = b.totalHistogram.TotalCount()

	//SetupTotalWrites
	configs["SetupWrites"] = b.setupWriteHistogram.TotalCount()

	//TotalWrites
	configs["Writes"] = b.writeHistogram.TotalCount()

	//TotalReads
	configs["Reads"] = b.readHistogram.TotalCount()

	//TotalReadsCursor
	configs["ReadsCursor"] = b.readCursorHistogram.TotalCount()

	//TotalUpdates
	configs["Updates"] = b.updateHistogram.TotalCount()

	//TotalDeletes
	configs["Deletes"] = b.deleteHistogram.TotalCount()

	//TotalTxBytes
	configs["TxBytes"] = b.txTotalBytes

	//TotalRxBytes
	configs["RxBytes"] = b.rxTotalBytes
	//
	//for k, _ := range b.detailedMapHistograms {
	//	fmt.Println(k)
	//	//configs[k] = v.TotalCount()
	//}

	return configs
}

func (b *BenchmarkRunner) GetMeasuredRatiosMap() map[string]interface{} {
	/////////
	// Overall Ratios
	/////////
	configs := map[string]interface{}{}

	totalOps := b.totalHistogram.TotalCount()
	writeRatio := float64(b.writeHistogram.TotalCount()+b.setupWriteHistogram.TotalCount()) / float64(totalOps)
	readRatio := float64(b.readHistogram.TotalCount()+b.readCursorHistogram.TotalCount()) / float64(totalOps)
	updateRatio := float64(b.updateHistogram.TotalCount()) / float64(totalOps)
	deleteRatio := float64(b.deleteHistogram.TotalCount()) / float64(totalOps)

	//MeasuredWriteRatio
	configs["MeasuredWriteRatio"] = writeRatio

	//MeasuredReadRatio
	configs["MeasuredReadRatio"] = readRatio

	//MeasuredUpdateRatio
	configs["MeasuredUpdateRatio"] = updateRatio

	//MeasuredDeleteRatio
	configs["MeasuredDeleteRatio"] = deleteRatio

	return configs
}

func (l *BenchmarkRunner) GetOverallRatesMap() map[string]interface{} {
	/////////
	// Overall Rates
	/////////
	configs := map[string]interface{}{}

	took := l.end.Sub(l.start)
	writeCount := l.writeHistogram.TotalCount()
	setupWriteCount := l.setupWriteHistogram.TotalCount()
	totalWriteCount := writeCount + setupWriteCount
	readCount := l.readHistogram.TotalCount()
	readCursorCount := l.readCursorHistogram.TotalCount()
	totalReadCount := readCount + readCursorCount
	updateCount := l.updateHistogram.TotalCount()
	deleteCount := l.deleteHistogram.TotalCount()

	totalOps := totalWriteCount + totalReadCount + updateCount + deleteCount
	txTotalBytes := atomic.LoadUint64(&l.txTotalBytes)
	rxTotalBytes := atomic.LoadUint64(&l.rxTotalBytes)

	setupWriteRate := calculateRateMetrics(setupWriteCount, 0, took)
	configs["setupWriteRate"] = setupWriteRate

	writeRate := calculateRateMetrics(writeCount, 0, took)
	configs["writeRate"] = writeRate

	readRate := calculateRateMetrics(readCount, 0, took)
	configs["readRate"] = readRate

	readCursorRate := calculateRateMetrics(readCursorCount, 0, took)
	configs["readCursorRate"] = readCursorRate

	updateRate := calculateRateMetrics(updateCount, 0, took)
	configs["updateRate"] = updateRate

	deleteRate := calculateRateMetrics(deleteCount, 0, took)
	configs["deleteRate"] = deleteRate

	overallOpsRate := calculateRateMetrics(totalOps, 0, took)
	configs["overallOpsRate"] = overallOpsRate

	for k, v := range l.detailedMapHistograms {
		rateStr := k + "Rate"
		count := v.TotalCount()
		rate := calculateRateMetrics(count, 0, took)
		configs[rateStr] = rate
	}

	overallTxByteRate := calculateRateMetrics(int64(txTotalBytes), 0, took)
	configs["overallTxByteRate"] = overallTxByteRate

	overallRxByteRate := calculateRateMetrics(int64(rxTotalBytes), 0, took)
	configs["overallRxByteRate"] = overallRxByteRate

	txByteRateStr := bytefmt.ByteSize(uint64(overallTxByteRate))
	configs["txByteRateStr"] = txByteRateStr

	rxByteRateStr := bytefmt.ByteSize(uint64(overallRxByteRate))
	configs["rxByteRateStr"] = rxByteRateStr
	return configs
}

func (b *BenchmarkRunner) GetTimeSeriesMap() map[string]interface{} {

	configs := map[string]interface{}{}
	sort.Sort(ByTimestamp(b.setupWriteTs))
	sort.Sort(ByTimestamp(b.writeTs))
	sort.Sort(ByTimestamp(b.readTs))
	sort.Sort(ByTimestamp(b.readCursorTs))
	sort.Sort(ByTimestamp(b.updateTs))
	sort.Sort(ByTimestamp(b.deleteTs))

	configs["setupWriteTs"] = b.setupWriteTs
	configs["writeTs"] = b.writeTs
	configs["readTs"] = b.readTs
	configs["readCursorTs"] = b.readCursorTs
	configs["updateTs"] = b.updateTs
	configs["deleteTs"] = b.deleteTs

	return configs
}

func (b *BenchmarkRunner) GetPerSecondEncodedHistogramsMap() map[uint64]string {
	configs := map[uint64]string{}
	for k := range b.perSecondHistograms {
		encodedV, _ := b.perSecondHistograms[k].Encode(hdrhistogram.V2CompressedEncodingCookieBase)
		configs[k] = string(encodedV)
	}
	return configs
}

var loader = &BenchmarkRunner{
	setupWriteHistogram:      hdrhistogram.New(1, 1000000, 3),
	inst_setupWriteHistogram: hdrhistogram.New(1, 1000000, 3),
	setupWriteTs:             make([]DataPoint, 0, 10),
	writeHistogram:           hdrhistogram.New(1, 1000000, 3),
	inst_writeHistogram:      hdrhistogram.New(1, 1000000, 3),
	writeTs:                  make([]DataPoint, 0, 10),
	updateHistogram:          hdrhistogram.New(1, 1000000, 3),
	inst_updateHistogram:     hdrhistogram.New(1, 1000000, 3),
	updateTs:                 make([]DataPoint, 0, 10),
	readHistogram:            hdrhistogram.New(1, 1000000, 3),
	inst_readHistogram:       hdrhistogram.New(1, 1000000, 3),
	readTs:                   make([]DataPoint, 0, 10),
	readCursorHistogram:      hdrhistogram.New(1, 1000000, 3),
	inst_readCursorHistogram: hdrhistogram.New(1, 1000000, 3),
	readCursorTs:             make([]DataPoint, 0, 10),
	deleteHistogram:          hdrhistogram.New(1, 1000000, 3),
	inst_deleteHistogram:     hdrhistogram.New(1, 1000000, 3),
	deleteTs:                 make([]DataPoint, 0, 10),
	totalHistogram:           hdrhistogram.New(1, 1000000, 3),
	inst_totalHistogram:      hdrhistogram.New(1, 1000000, 3),
	totalTs:                  make([]DataPoint, 0, 10),
	detailedMapHistograms:    make(map[string]*hdrhistogram.Histogram),
	perSecondHistograms:      make(map[uint64]*hdrhistogram.Histogram),
}

// GetBenchmarkRunner returns the singleton BenchmarkRunner for use in a benchmark program
// with a default batch size
func GetBenchmarkRunner() *BenchmarkRunner {
	return GetBenchmarkRunnerWithBatchSize(defaultBatchSize)
}

// GetBenchmarkRunnerWithBatchSize returns the singleton BenchmarkRunner for use in a benchmark program
// with specified batch size.
func GetBenchmarkRunnerWithBatchSize(batchSize uint) *BenchmarkRunner {
	// fill flag fields of BenchmarkRunner struct
	flag.UintVar(&loader.workers, "workers", 8, "Number of parallel clients inserting")
	flag.Uint64Var(&loader.limit, "requests", 0, "Number of total requests to issue (0 = all of the present in input file).")
	flag.BoolVar(&loader.doLoad, "do-benchmark", true, "Whether to write databuild. Set this flag to false to check input read speed.")
	flag.DurationVar(&loader.reportingPeriod, "reporting-period", 1*time.Second, "Period to report write stats")
	flag.StringVar(&loader.fileName, "input", "", "File name to read databuild from")
	flag.Uint64Var(&loader.maxRPS, "max-rps", 0, "enable limiting the rate of queries per second, 0 = no limit. By default no limit is specified and the binaries will stress the DB up to the maximum. A normal \"modus operandi\" would be to initially stress the system ( no limit on RPS) and afterwards that we know the limit vary with lower rps configurations.")
	flag.StringVar(&loader.JsonOutFile, "json-out-file", "", "Name of json output file to output benchmark results. If not set, will not print to json.")
	flag.StringVar(&loader.Metadata, "metadata-string", "", "Metadata string to add to json-out-file. If -json-out-file is not set, will not use this option.")
	return loader
}

// RunBenchmark takes in a Benchmark b, a bufio.Reader br, and holders for number of metrics and rows
// and reads those to run the benchmark benchmark
func (l *BenchmarkRunner) RunBenchmark(b Benchmark, workQueues uint) {
	l.br = l.GetBufferedReader()

	channels := l.createChannels(workQueues)
	// Launch all worker processes in background

	var requestRate = Inf
	var requestBurst = 1
	if l.maxRPS != 0 {
		requestRate = rate.Limit(l.maxRPS)
		requestBurst = int(l.workers) //int(b.workers)
	}
	var rateLimiter = rate.NewLimiter(requestRate, requestBurst)

	var wg sync.WaitGroup
	for i := 0; i < int(l.workers); i++ {
		wg.Add(1)
		go l.work(b, &wg, channels[i%len(channels)], i, rateLimiter, l.maxRPS != 0)
	}

	w := new(tabwriter.Writer)
	w.Init(os.Stderr, 20, 0, 0, ' ', tabwriter.AlignRight)
	// Start scan process - actual databuild read process
	l.start = time.Now()

	l.scan(b, channels, l.start, w)

	// After scan process completed (no more databuild to come) - begin shutdown process

	// Close all communication channels to/from workers
	for _, c := range channels {
		c.close()
	}

	// Wait for all workers to finish
	wg.Wait()
	l.end = time.Now()
	l.testResult.DBSpecificConfigs = b.GetConfigurationParametersMap()
	l.testResult.Totals = l.GetTotalsMap()
	l.testResult.MeasuredRatios = l.GetMeasuredRatiosMap()
	l.testResult.OverallRates = l.GetOverallRatesMap()
	l.testResult.TimeSeries = l.GetTimeSeriesMap()
	l.testResult.OverallQuantiles = l.GetOverallQuantiles()
	l.testResult.PerSecondEncodedHistograms = l.GetPerSecondEncodedHistogramsMap()
	l.testResult.Limit = l.limit
	l.testResult.Workers = l.workers
	l.testResult.MaxRps = l.maxRPS
	l.summary()
}

// GetBufferedReader returns the buffered Reader that should be used by the loader
func (l *BenchmarkRunner) GetBufferedReader() *bufio.Reader {
	if l.br == nil {
		if len(l.fileName) > 0 {
			// Read from specified file
			file, err := os.Open(l.fileName)
			if err != nil {
				log.Fatalf("cannot open file for read %s: %v", l.fileName, err)
				return nil
			}
			l.br = bufio.NewReaderSize(file, defaultReadSize)
		} else {
			// Read from STDIN
			l.br = bufio.NewReaderSize(os.Stdin, defaultReadSize)
		}
	}
	return l.br
}

// createChannels create channels from which workers would receive tasks
// Number of workers may be different from number of channels, thus we may have
// multiple workers per channel
func (l *BenchmarkRunner) createChannels(workQueues uint) []*duplexChannel {
	// Result - channels to be created
	channels := []*duplexChannel{}

	// How many work queues should be created?
	workQueuesToCreate := workQueues
	if workQueues == WorkerPerQueue {
		workQueuesToCreate = l.workers
	} else if workQueues > l.workers {
		panic(fmt.Sprintf("cannot have more work queues (%d) than workers (%d)", workQueues, l.workers))
	}

	// How many workers would be served by each queue?
	workersPerQueue := int(math.Ceil(float64(l.workers) / float64(workQueuesToCreate)))

	// Create duplex communication channels
	for i := uint(0); i < workQueuesToCreate; i++ {
		channels = append(channels, newDuplexChannel(workersPerQueue))
	}

	return channels
}

// scan launches any needed reporting mechanism and proceeds to scan input databuild
// to distribute to workers
func (l *BenchmarkRunner) scan(b Benchmark, channels []*duplexChannel, start time.Time, w *tabwriter.Writer) uint64 {
	// Start background reporting process
	// TODO why it is here? May be it could be moved one level up?
	if l.reportingPeriod.Nanoseconds() > 0 {
		go l.report(l.reportingPeriod, start, w)
	}

	// Scan incoming databuild
	return scanWithIndexer(channels, 100, l.limit, l.br, b.GetCmdDecoder(l.br), b.GetBatchFactory(), b.GetCommandIndexer(uint(len(channels))))
}

// work is the processing function for each worker in the loader
func (l *BenchmarkRunner) work(b Benchmark, wg *sync.WaitGroup, c *duplexChannel, workerNum int, rateLimiter *rate.Limiter, useRateLimiter bool) {

	// Prepare processor
	proc := b.GetProcessor()
	proc.Init(workerNum, l.doLoad, int(l.workers))

	// Process batches coming from duplexChannel.toWorker queue
	// and send ACKs into duplexChannel.toScanner queue
	for b := range c.toWorker {
		stats := proc.ProcessBatch(b, l.doLoad, rateLimiter, useRateLimiter)
		cmdStats := stats.CmdStats()
		for pos := 0; pos < len(cmdStats); pos++ {
			cmdStat := cmdStats[pos]
			_ = l.totalHistogram.RecordValue(int64(cmdStat.Latency()))
			_ = l.inst_totalHistogram.RecordValue(int64(cmdStat.Latency()))

			atomic.AddUint64(&l.txTotalBytes, cmdStat.Tx())
			atomic.AddUint64(&l.rxTotalBytes, cmdStat.Rx())
			labelStr := string(cmdStat.Label())
			querystr := string(cmdStat.CmdQueryId())
			groupAndQuery := labelStr + "-" + querystr
			l.detailedMapHistogramsMutex.Lock()
			if _, exist := l.detailedMapHistograms[groupAndQuery]; !exist {
				l.detailedMapHistograms[groupAndQuery] = hdrhistogram.New(1, 1000000, 3)
			}
			l.detailedMapHistograms[groupAndQuery].RecordValue(int64(cmdStat.Latency()))
			l.detailedMapHistogramsMutex.Unlock()

			ts := cmdStat.StartTs()
			l.perSecondHistogramsMutex.Lock()
			if _, exist := l.perSecondHistograms[ts]; !exist {
				l.perSecondHistograms[ts] = hdrhistogram.New(1, 1000000, 3)
			}
			l.perSecondHistograms[ts].RecordValue(int64(cmdStat.Latency()))
			l.perSecondHistogramsMutex.Unlock()

			switch labelStr {
			case "SETUP_WRITE":
				_ = l.setupWriteHistogram.RecordValue(int64(cmdStat.Latency()))
				_ = l.inst_setupWriteHistogram.RecordValue(int64(cmdStat.Latency()))

				break
			case "WRITE":
				_ = l.writeHistogram.RecordValue(int64(cmdStat.Latency()))
				_ = l.inst_writeHistogram.RecordValue(int64(cmdStat.Latency()))

				break
			case "UPDATE":
				_ = l.updateHistogram.RecordValue(int64(cmdStat.Latency()))
				_ = l.inst_updateHistogram.RecordValue(int64(cmdStat.Latency()))

				break
			case "READ":
				_ = l.readHistogram.RecordValue(int64(cmdStat.Latency()))
				_ = l.inst_readHistogram.RecordValue(int64(cmdStat.Latency()))

				break
			case "CURSOR_READ":
				_ = l.readCursorHistogram.RecordValue(int64(cmdStat.Latency()))
				_ = l.inst_readCursorHistogram.RecordValue(int64(cmdStat.Latency()))

				break
			case "DELETE":
				_ = l.deleteHistogram.RecordValue(int64(cmdStat.Latency()))
				_ = l.inst_deleteHistogram.RecordValue(int64(cmdStat.Latency()))

				break
			}
		}
		c.sendToScanner()
	}

	// Close proc if necessary
	switch c := proc.(type) {
	case ProcessorCloser:
		c.Close(l.doLoad)
	}

	wg.Done()
}

// summary prints the summary of statistics from loading
func (l *BenchmarkRunner) summary() {
	took := l.end.Sub(l.start)
	writeCount := l.writeHistogram.TotalCount()
	setupWriteCount := l.setupWriteHistogram.TotalCount()
	totalWriteCount := writeCount + setupWriteCount
	readCount := l.readHistogram.TotalCount()
	readCursorCount := l.readCursorHistogram.TotalCount()
	totalReadCount := readCount + readCursorCount
	updateCount := l.updateHistogram.TotalCount()
	deleteCount := l.deleteHistogram.TotalCount()

	totalOps := totalWriteCount + totalReadCount + updateCount + deleteCount
	txTotalBytes := atomic.LoadUint64(&l.txTotalBytes)
	rxTotalBytes := atomic.LoadUint64(&l.rxTotalBytes)

	setupWriteRate := calculateRateMetrics(setupWriteCount, 0, took)
	writeRate := calculateRateMetrics(writeCount, 0, took)
	readRate := calculateRateMetrics(readCount, 0, took)
	readCursorRate := calculateRateMetrics(readCursorCount, 0, took)
	updateRate := calculateRateMetrics(updateCount, 0, took)
	deleteRate := calculateRateMetrics(deleteCount, 0, took)
	overallOpsRate := calculateRateMetrics(totalOps, 0, took)
	overallTxByteRate := calculateRateMetrics(int64(txTotalBytes), 0, took)
	overallRxByteRate := calculateRateMetrics(int64(rxTotalBytes), 0, took)
	txByteRateStr := bytefmt.ByteSize(uint64(overallTxByteRate))
	rxByteRateStr := bytefmt.ByteSize(uint64(overallRxByteRate))

	/////////
	// Totals
	/////////
	l.testResult.StartTime = l.start.Unix() * 1000
	l.testResult.EndTime = l.end.Unix() * 1000
	l.testResult.DurationMillis = took.Milliseconds()
	l.testResult.Metadata = l.Metadata
	l.testResult.ResultFormatVersion = CurrentResultFormatVersion

	fmt.Printf("\nSummary:\n")
	fmt.Printf("Issued %d Commands in %0.3fsec with %d workers\n", totalOps, took.Seconds(), l.workers)
	fmt.Printf("\tOverall stats:\n\t"+
		"- Total %0.0f ops/sec\t\t\tq50 lat %0.3f ms\n\t"+
		"- Setup Writes %0.0f ops/sec\t\tq50 lat %0.3f ms\n\t"+
		"- Writes %0.0f ops/sec\t\t\tq50 lat %0.3f ms\n\t"+
		"- Reads %0.0f ops/sec\t\t\tq50 lat %0.3f ms\n\t"+
		"- Cursor Reads %0.0f ops/sec\t\tq50 lat %0.3f ms\n\t"+
		"- Updates %0.0f ops/sec\t\t\tq50 lat %0.3f ms\n\t"+
		"- Deletes %0.0f ops/sec\t\t\tq50 lat %0.3f ms\n",
		overallOpsRate,
		float64(l.totalHistogram.ValueAtQuantile(50.0))/10e2,
		setupWriteRate,
		float64(l.setupWriteHistogram.ValueAtQuantile(50.0))/10e2,
		writeRate,
		float64(l.writeHistogram.ValueAtQuantile(50.0))/10e2,
		readRate,
		float64(l.readHistogram.ValueAtQuantile(50.0))/10e2,
		readCursorRate,
		float64(l.readCursorHistogram.ValueAtQuantile(50.0))/10e2,
		updateRate,
		float64(l.updateHistogram.ValueAtQuantile(50.0))/10e2,
		deleteRate,
		float64(l.deleteHistogram.ValueAtQuantile(50.0))/10e2,
	)
	fmt.Printf("\tOverall TX Byte Rate: %sB/sec\n", txByteRateStr)
	fmt.Printf("\tOverall RX Byte Rate: %sB/sec\n", rxByteRateStr)

	if strings.Compare(l.JsonOutFile, "") != 0 {

		file, err := json.MarshalIndent(l.testResult, "", " ")
		if err != nil {
			log.Fatal(err)
		}

		err = ioutil.WriteFile(l.JsonOutFile, file, 0644)
		if err != nil {
			log.Fatal(err)
		}
	}

}

// report handles periodic reporting of loading stats
func (l *BenchmarkRunner) report(period time.Duration, start time.Time, w *tabwriter.Writer) {
	prevTime := start
	prevWriteCount := int64(0)
	prevSetupWriteCount := int64(0)
	prevUpdateCount := int64(0)
	prevReadCursorCount := int64(0)
	prevReadCount := int64(0)
	prevDeleteCount := int64(0)
	prevTotalOps := int64(0)
	prevTxTotalBytes := uint64(0)
	prevRxTotalBytes := uint64(0)

	fmt.Fprint(w, "setup writes/sec\twrites/sec\tupdates/sec\treads/sec\tcursor reads/sec\tdeletes/sec\tcurrent ops/sec\ttotal ops\tTX BW/s\tRX BW/s\n")
	w.Flush()
	for now := range time.NewTicker(period).C {
		took := now.Sub(prevTime)
		writeCount := l.writeHistogram.TotalCount()
		setupWriteCount := l.setupWriteHistogram.TotalCount()
		totalWriteCount := writeCount + setupWriteCount
		readCount := l.readHistogram.TotalCount()
		readCursorCount := l.readCursorHistogram.TotalCount()
		totalReadCount := readCount + readCursorCount
		updateCount := l.updateHistogram.TotalCount()
		deleteCount := l.deleteHistogram.TotalCount()

		totalOps := totalWriteCount + totalReadCount + updateCount + deleteCount
		txTotalBytes := atomic.LoadUint64(&l.txTotalBytes)
		rxTotalBytes := atomic.LoadUint64(&l.rxTotalBytes)
		setupWriteRate := calculateRateMetrics(setupWriteCount, prevSetupWriteCount, took)
		writeRate := calculateRateMetrics(writeCount, prevWriteCount, took)
		readRate := calculateRateMetrics(readCount, prevReadCount, took)
		readCursorRate := calculateRateMetrics(readCursorCount, prevReadCursorCount, took)
		updateRate := calculateRateMetrics(updateCount, prevUpdateCount, took)
		deleteRate := calculateRateMetrics(deleteCount, prevDeleteCount, took)
		CurrentOpsRate := calculateRateMetrics(totalOps, prevTotalOps, took)
		overallTxByteRate := calculateRateMetrics(int64(txTotalBytes), int64(prevTxTotalBytes), took)
		overallRxByteRate := calculateRateMetrics(int64(rxTotalBytes), int64(prevRxTotalBytes), took)
		txByteRateStr := bytefmt.ByteSize(uint64(overallTxByteRate))
		rxByteRateStr := bytefmt.ByteSize(uint64(overallRxByteRate))

		l.setupWriteTs = l.addRateMetricsDatapoints(l.setupWriteTs, now, took, l.inst_setupWriteHistogram)
		l.writeTs = l.addRateMetricsDatapoints(l.writeTs, now, took, l.inst_writeHistogram)
		l.readTs = l.addRateMetricsDatapoints(l.readTs, now, took, l.inst_readHistogram)
		l.readCursorTs = l.addRateMetricsDatapoints(l.readCursorTs, now, took, l.inst_readCursorHistogram)
		l.updateTs = l.addRateMetricsDatapoints(l.updateTs, now, took, l.inst_updateHistogram)
		l.deleteTs = l.addRateMetricsDatapoints(l.deleteTs, now, took, l.inst_deleteHistogram)

		fmt.Fprint(w, fmt.Sprintf("%.0f (%.3f) \t%.0f (%.3f) \t%.0f (%.3f) \t%.0f (%.3f) \t%.0f (%.3f) \t%.0f (%.3f) \t %.0f (%.3f) \t%d \t %sB/s \t %sB/s\n",
			setupWriteRate,
			float64(l.setupWriteHistogram.ValueAtQuantile(50.0))/10e2,

			writeRate,
			float64(l.writeHistogram.ValueAtQuantile(50.0))/10e2,

			updateRate,
			float64(l.updateHistogram.ValueAtQuantile(50.0))/10e2,

			readRate,
			float64(l.readHistogram.ValueAtQuantile(50.0))/10e2,

			readCursorRate,
			float64(l.readCursorHistogram.ValueAtQuantile(50.0))/10e2,

			deleteRate,
			float64(l.deleteHistogram.ValueAtQuantile(50.0))/10e2,

			CurrentOpsRate,
			float64(l.totalHistogram.ValueAtQuantile(50.0))/10e2,
			totalOps, txByteRateStr, rxByteRateStr))
		w.Flush()
		prevSetupWriteCount = setupWriteCount
		prevWriteCount = writeCount
		prevReadCount = readCount
		prevReadCursorCount = readCursorCount
		prevUpdateCount = updateCount
		prevDeleteCount = deleteCount
		prevTxTotalBytes = txTotalBytes
		prevRxTotalBytes = rxTotalBytes
		prevTotalOps = totalOps
		prevTime = now

		l.inst_setupWriteHistogram.Reset()
		l.inst_writeHistogram.Reset()
		l.inst_readHistogram.Reset()
		l.inst_readCursorHistogram.Reset()
		l.inst_updateHistogram.Reset()
		l.inst_deleteHistogram.Reset()

	}
}

// protect against NaN on json
func wrapNaN(input float64) (output float64) {
	output = input
	if math.IsNaN(output) {
		output = -1.0
	}
	return
}

func (l *BenchmarkRunner) addRateMetricsDatapoints(datapoints []DataPoint, now time.Time, timeframe time.Duration, hist *hdrhistogram.Histogram) []DataPoint {
	ops, mp := generateQuantileMap(hist)
	rate := 0.0
	rate = float64(ops) / float64(timeframe.Seconds())
	mp["rate"] = rate
	datapoint := DataPoint{now.Unix(), mp}
	datapoints = append(datapoints, datapoint)
	return datapoints

}

func generateQuantileMap(hist *hdrhistogram.Histogram) (int64, map[string]float64) {
	ops := hist.TotalCount()
	q0 := 0.0
	q50 := 0.0
	q95 := 0.0
	q99 := 0.0
	q999 := 0.0
	q100 := 0.0
	if ops > 0 {
		q0 = float64(hist.ValueAtQuantile(0.0)) / 10e2
		q50 = float64(hist.ValueAtQuantile(50.0)) / 10e2
		q95 = float64(hist.ValueAtQuantile(95.0)) / 10e2
		q99 = float64(hist.ValueAtQuantile(99.0)) / 10e2
		q999 = float64(hist.ValueAtQuantile(99.90)) / 10e2
		q100 = float64(hist.ValueAtQuantile(100.0)) / 10e2
	}

	mp := map[string]float64{"q0": q0, "q50": q50, "q95": q95, "q99": q99, "q999": q999, "q100": q100}
	return ops, mp
}

func (b *BenchmarkRunner) GetOverallQuantiles() map[string]interface{} {
	configs := map[string]interface{}{}
	_, setupWrite := generateQuantileMap(b.setupWriteHistogram)
	configs["setupWrite"] = setupWrite
	_, write := generateQuantileMap(b.writeHistogram)
	configs["write"] = write
	_, read := generateQuantileMap(b.readHistogram)
	configs["read"] = read
	_, readCursor := generateQuantileMap(b.readCursorHistogram)
	configs["readCursor"] = readCursor
	_, update := generateQuantileMap(b.updateHistogram)
	configs["update"] = update
	_, delete := generateQuantileMap(b.deleteHistogram)
	configs["delete"] = delete
	_, all := generateQuantileMap(b.totalHistogram)
	configs["allCommands"] = all

	for k, hist := range b.detailedMapHistograms {
		_, quantilesMap := generateQuantileMap(hist)
		configs[k] = quantilesMap
	}

	return configs
}

func calculateRateMetrics(current, prev int64, took time.Duration) (rate float64) {
	rate = float64(current-prev) / float64(took.Seconds())
	return
}
