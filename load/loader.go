package load

import (
	"bufio"
	"code.cloudfoundry.org/bytefmt"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/filipecosta90/hdrhistogram"
	"io/ioutil"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"
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

	errDBExistsFmt = "database \"%s\" exists: aborting."
)

// change for more useful testing
var (
	printFn = fmt.Printf
	fatal   = log.Fatalf
)

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

	// GetDBCreator returns the DBCreator to use for this Benchmark
	GetDBCreator() DBCreator

	// GetConfigurationParametersMap returns the map of specific configurations used in the benchmark
	GetConfigurationParametersMap() map[string]interface{}
}

// BenchmarkRunner is responsible for initializing and storing common
// flags across all database systems and ultimately running a supplied Benchmark
type BenchmarkRunner struct {
	// flag fields
	dbName          string
	JsonOutFile     string
	Metadata        string
	batchSize       uint
	workers         uint
	limit           uint64
	doLoad          bool
	doCreateDB      bool
	doAbortOnExist  bool
	reportingPeriod time.Duration
	fileName        string

	// non-flag fields
	br                  *bufio.Reader
	setupWriteCount     uint64
	setupWriteHistogram *hdrhistogram.Histogram
	writeCount          uint64
	writeHistogram      *hdrhistogram.Histogram
	updateCount         uint64
	updateHistogram     *hdrhistogram.Histogram
	readCount           uint64
	readHistogram       *hdrhistogram.Histogram
	readCursorCount     uint64
	readCursorHistogram *hdrhistogram.Histogram
	deleteCount         uint64
	deleteHistogram     *hdrhistogram.Histogram
	totalLatency        uint64
	totalHistogram      *hdrhistogram.Histogram
	txTotalBytes        uint64
	rxTotalBytes        uint64

	testResult TestResult
}

var loader = &BenchmarkRunner{
	setupWriteHistogram: hdrhistogram.New(1, 1000000, 4),
	writeHistogram:      hdrhistogram.New(1, 1000000, 4),
	updateHistogram:     hdrhistogram.New(1, 1000000, 4),
	readHistogram:       hdrhistogram.New(1, 1000000, 4),
	readCursorHistogram: hdrhistogram.New(1, 1000000, 4),
	deleteHistogram:     hdrhistogram.New(1, 1000000, 4),
	totalHistogram:      hdrhistogram.New(1, 1000000, 4),
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
	flag.StringVar(&loader.dbName, "index", "idx1", "Name of index")
	flag.UintVar(&loader.batchSize, "batch-size", batchSize, "Number of items to batch together in a single insert")
	flag.UintVar(&loader.workers, "workers", 8, "Number of parallel clients inserting")
	flag.Uint64Var(&loader.limit, "limit", 0, "Number of items to insert (0 = all of them).")
	flag.BoolVar(&loader.doLoad, "do-benchmark", true, "Whether to write databuild. Set this flag to false to check input read speed.")
	flag.BoolVar(&loader.doCreateDB, "do-create-db", true, "Whether to create the database. Disable on all but one client if running on a multi client setup.")
	flag.BoolVar(&loader.doAbortOnExist, "do-abort-on-exist", false, "Whether to abort if a database with the given name already exists.")
	flag.DurationVar(&loader.reportingPeriod, "reporting-period", 1*time.Second, "Period to report write stats")
	flag.StringVar(&loader.fileName, "file", "", "File name to read databuild from")
	flag.StringVar(&loader.JsonOutFile, "json-config-file", "", "Name of json config file to read the setup/teardown info. If not set, will not do any of those and simple issue the commands from --file.")
	flag.StringVar(&loader.JsonOutFile, "json-out-file", "", "Name of json output file to output benchmark results. If not set, will not print to json.")
	flag.StringVar(&loader.Metadata, "metadata-string", "", "Metadata string to add to json-out-file. If -json-out-file is not set, will not use this option.")
	return loader
}

// DatabaseName returns the value of the --db-name flag (name of the database to store databuild)
func (l *BenchmarkRunner) DatabaseName() string {
	return l.dbName
}

// RunBenchmark takes in a Benchmark b, a bufio.Reader br, and holders for number of metrics and rows
// and reads those to run the benchmark benchmark
func (l *BenchmarkRunner) RunBenchmark(b Benchmark, workQueues uint) {
	l.br = l.GetBufferedReader()

	// Create required DB
	cleanupFn := l.useDBCreator(b.GetDBCreator())
	defer cleanupFn()

	channels := l.createChannels(workQueues)
	// Launch all worker processes in background
	var wg sync.WaitGroup
	for i := 0; i < int(l.workers); i++ {
		wg.Add(1)
		go l.work(b, &wg, channels[i%len(channels)], i)
	}

	w := new(tabwriter.Writer)
	w.Init(os.Stderr, 20, 0, 0, ' ', tabwriter.AlignRight)
	// Start scan process - actual databuild read process
	start := time.Now()

	l.scan(b, channels, start, w)

	// After scan process completed (no more databuild to come) - begin shutdown process

	// Close all communication channels to/from workers
	for _, c := range channels {
		c.close()
	}

	// Wait for all workers to finish
	wg.Wait()
	end := time.Now()
	l.testResult.DBSpecificConfigs = b.GetConfigurationParametersMap()
	l.testResult.Limit = l.limit
	l.testResult.DbName = l.dbName
	l.testResult.Workers = l.workers
	l.summary(start, end)
}

// GetBufferedReader returns the buffered Reader that should be used by the loader
func (l *BenchmarkRunner) GetBufferedReader() *bufio.Reader {
	if l.br == nil {
		if len(l.fileName) > 0 {
			// Read from specified file
			file, err := os.Open(l.fileName)
			if err != nil {
				fatal("cannot open file for read %s: %v", l.fileName, err)
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

// useDBCreator handles a DBCreator by running it according to flags set by the
// user. The function returns a function that the caller should defer or run
// when the benchmark is finished
func (l *BenchmarkRunner) useDBCreator(dbc DBCreator) func() {
	// Empty function to 'defer' from caller
	closeFn := func() {}

	if l.doLoad {
		// DBCreator should still be Init'd even if -do-create-db is false since
		// it can initialize the connecting session
		dbc.Init()

		switch dbcc := dbc.(type) {
		case DBCreatorCloser:
			closeFn = dbcc.Close
		}

		// Check whether required DB already exists
		exists := dbc.DBExists(l.dbName)
		if exists && l.doAbortOnExist {
			panic(fmt.Sprintf(errDBExistsFmt, l.dbName))
		}

		// Create required DB if need be
		// In case DB already exists - delete it
		if l.doCreateDB {
			if exists {
				err := dbc.RemoveOldDB(l.dbName)
				if err != nil {
					panic(err)
				}
			}
			err := dbc.CreateDB(l.dbName)
			if err != nil {
				panic(err)
			}
		}

		switch dbcp := dbc.(type) {
		case DBCreatorPost:
			dbcp.PostCreateDB(l.dbName)
		}
	}
	return closeFn
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
	return scanWithIndexer(channels, l.batchSize, l.limit, l.br, b.GetCmdDecoder(l.br), b.GetBatchFactory(), b.GetCommandIndexer(uint(len(channels))))
}

// work is the processing function for each worker in the loader
func (l *BenchmarkRunner) work(b Benchmark, wg *sync.WaitGroup, c *duplexChannel, workerNum int) {

	// Prepare processor
	proc := b.GetProcessor()
	proc.Init(workerNum, l.doLoad, int(l.workers))

	// Process batches coming from duplexChannel.toWorker queue
	// and send ACKs into duplexChannel.toScanner queue
	for b := range c.toWorker {
		stats := proc.ProcessBatch(b, l.doLoad)
		cmdStats := stats.CmdStats()
		for pos := 0; pos < len(cmdStats); pos++ {
			cmdStat := cmdStats[pos]
			atomic.AddUint64(&l.totalLatency, cmdStat.Latency())
			_ = l.totalHistogram.RecordValue(int64(cmdStat.Latency()))
			atomic.AddUint64(&l.txTotalBytes, cmdStat.Tx())
			atomic.AddUint64(&l.rxTotalBytes, cmdStat.Rx())
			labelStr := string(cmdStat.Label())
			switch labelStr {
			case "SETUP_WRITE":
				atomic.AddUint64(&l.setupWriteCount, 1)
				_ = l.setupWriteHistogram.RecordValue(int64(cmdStat.Latency()))
				break
			case "WRITE":
				atomic.AddUint64(&l.writeCount, 1)
				_ = l.writeHistogram.RecordValue(int64(cmdStat.Latency()))
				break
			case "UPDATE":
				atomic.AddUint64(&l.updateCount, 1)
				_ = l.updateHistogram.RecordValue(int64(cmdStat.Latency()))
				break
			case "READ":
				atomic.AddUint64(&l.readCount, 1)
				_ = l.readHistogram.RecordValue(int64(cmdStat.Latency()))
				break
			case "CURSOR_READ":
				atomic.AddUint64(&l.readCursorCount, 1)
				_ = l.readCursorHistogram.RecordValue(int64(cmdStat.Latency()))
				break
			case "DELETE":
				atomic.AddUint64(&l.deleteCount, 1)
				_ = l.deleteHistogram.RecordValue(int64(cmdStat.Latency()))
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
func (l *BenchmarkRunner) summary(start time.Time, end time.Time) {
	took := end.Sub(start)
	writeCount := atomic.LoadUint64(&l.writeCount)
	setupWriteCount := atomic.LoadUint64(&l.setupWriteCount)
	totalWriteCount := writeCount + setupWriteCount

	readCount := atomic.LoadUint64(&l.readCount)
	readCursorCount := atomic.LoadUint64(&l.readCursorCount)
	totalReadCount := readCount + readCursorCount

	updateCount := atomic.LoadUint64(&l.updateCount)
	deleteCount := atomic.LoadUint64(&l.deleteCount)
	totalOps := totalWriteCount + totalReadCount + updateCount + deleteCount
	totalLatency := atomic.LoadUint64(&l.totalLatency)
	txTotalBytes := atomic.LoadUint64(&l.txTotalBytes)
	rxTotalBytes := atomic.LoadUint64(&l.rxTotalBytes)

	setupWriteRate, writeRate, readRate, readCursorRate, updateRate, deleteRate, _, overallOpsRate, _, overallAvgLatency, _, overallTxByteRate, _, overallRxByteRate := calculateRateMetrics(setupWriteCount, 0, writeCount, 0, totalReadCount, 0, readCursorCount, 0, updateCount, 0, deleteCount, 0, totalLatency, 0, txTotalBytes, 0, rxTotalBytes, 0, took, took)
	txByteRateStr := bytefmt.ByteSize(uint64(overallTxByteRate))
	rxByteRateStr := bytefmt.ByteSize(uint64(overallRxByteRate))

	/////////
	// Totals
	/////////
	l.testResult.StartTime = start.Unix()
	l.testResult.EndTime = end.Unix()
	l.testResult.DurationMillis = took.Milliseconds()
	l.testResult.BatchSize = int64(l.batchSize)
	l.testResult.Metadata = l.Metadata

	l.testResult.ResultFormatVersion = CurrentResultFormatVersion

	//TotalOps
	l.testResult.TotalOps = totalOps

	//SetupTotalWrites
	l.testResult.SetupTotalWrites = setupWriteCount

	//TotalWrites
	l.testResult.TotalWrites = writeCount

	//TotalReads
	l.testResult.TotalReads = readCount

	//TotalReadsCursor
	l.testResult.TotalReadsCursor = readCursorCount

	//TotalUpdates
	l.testResult.TotalUpdates = updateCount

	//TotalDeletes
	l.testResult.TotalDeletes = deleteCount

	//TotalLatency
	l.testResult.TotalLatency = totalLatency

	//TotalTxBytes
	l.testResult.TotalTxBytes = txTotalBytes

	//TotalTxBytes
	l.testResult.TotalRxBytes = rxTotalBytes

	/////////
	// Overall Ratios
	/////////

	//MeasuredWriteRatio
	l.testResult.MeasuredWriteRatio = float64(writeCount) / float64(totalOps)

	//MeasuredReadRatio
	l.testResult.MeasuredReadRatio = float64(readCount) / float64(totalOps)

	//MeasuredUpdateRatio
	l.testResult.MeasuredUpdateRatio = float64(updateCount) / float64(totalOps)

	//MeasuredDeleteRatio
	l.testResult.MeasuredDeleteRatio = float64(deleteCount) / float64(totalOps)

	/////////
	// Overall Rates
	/////////

	//OverallAvgIndexingRate
	l.testResult.OverallAvgOpsRate = overallOpsRate
	//OverallAvgWriteRate
	l.testResult.OverallAvgSetupWriteRate = setupWriteRate

	//OverallAvgWriteRate
	l.testResult.OverallAvgWriteRate = writeRate

	//OverallAvgUpdateRate
	l.testResult.OverallAvgUpdateRate = updateRate

	//OverallAvgDeleteRate
	l.testResult.OverallAvgDeleteRate = deleteRate

	//OverallAvgLatency
	l.testResult.OverallAvgLatency = overallAvgLatency

	//OverallAvgTxByteRate
	l.testResult.OverallAvgTxByteRate = overallTxByteRate
	l.testResult.OverallAvgByteRateHumanReadable = fmt.Sprintf("%sB/sec", txByteRateStr)

	printFn("\nSummary:\n")
	printFn("Issued %d Commands in %0.3fsec with %d workers\n", totalOps, took.Seconds(), l.workers)
	printFn("\tMean rate:\n\t "+
		"- Total %0.0f ops/sec %0.3f q50 lat\n\t"+
		"- Setup Writes %0.0f ops/sec%0.3f q50 lat\n\t"+
		"- Writes %0.0f ops/sec %0.3f q50 lat\n\t"+
		"- Reads %0.0f ops/sec %0.3f q50 lat\n\t"+
		"- Cursor Reads %0.0f ops/sec %0.3f q50 lat\n\t"+
		"- Updates %0.0f ops/sec %0.3f q50 lat\n\t"+
		"- Deletes %0.0f ops/sec %0.3f q50 lat\n\t",
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
	printFn("\tOverall Avg Latency: %0.3f msec\n", overallAvgLatency)
	printFn("\tOverall TX Byte Rate: %sB/sec\n", txByteRateStr)
	printFn("\tOverall RX Byte Rate: %sB/sec\n", rxByteRateStr)

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
	prevWriteCount := uint64(0)
	prevSetupWriteCount := uint64(0)
	prevUpdateCount := uint64(0)
	prevReadCursorCount := uint64(0)
	prevReadCount := uint64(0)
	prevDeleteCount := uint64(0)
	prevTotalLatency := uint64(0)
	prevTxTotalBytes := uint64(0)
	prevRxTotalBytes := uint64(0)

	fmt.Fprint(w, "setup writes/sec\twrites/sec\tupdates/sec\treads/sec\tcursor reads/sec\tdeletes/sec\tcurrent ops/sec\ttotal ops\tTX BW/s\tRX BW/s\n")
	w.Flush()
	for now := range time.NewTicker(period).C {
		setupWriteCount := atomic.LoadUint64(&l.setupWriteCount)
		writeCount := atomic.LoadUint64(&l.writeCount)
		updateCount := atomic.LoadUint64(&l.updateCount)
		readCount := atomic.LoadUint64(&l.readCount)
		readCursorCount := atomic.LoadUint64(&l.readCursorCount)
		deleteCount := atomic.LoadUint64(&l.deleteCount)
		totalOps := setupWriteCount + writeCount + updateCount + readCount + readCursorCount + deleteCount
		totalLatency := atomic.LoadUint64(&l.totalLatency)
		txTotalBytes := atomic.LoadUint64(&l.txTotalBytes)
		rxTotalBytes := atomic.LoadUint64(&l.rxTotalBytes)

		sinceStart := now.Sub(start)
		took := now.Sub(prevTime)
		setupWriteRate, writeRate, readRate, readCursorRate, updateRate, deleteRate, CurrentOpsRate, _, currentAvgLatency, _, currentTxByteRate, _, currentRxByteRate, _ := calculateRateMetrics(setupWriteCount, prevSetupWriteCount, writeCount, prevWriteCount, readCount, prevReadCount, readCursorCount, prevReadCursorCount, updateCount, prevUpdateCount, deleteCount, prevDeleteCount, totalLatency, prevTotalLatency, txTotalBytes, prevTxTotalBytes, rxTotalBytes, prevRxTotalBytes, took, sinceStart)
		currentTxByteRateStr := bytefmt.ByteSize(uint64(currentTxByteRate))
		currentRxByteRateStr := bytefmt.ByteSize(uint64(currentRxByteRate))

		l.addRateMetricsDatapoints(now, setupWriteRate, writeRate, readRate, readCursorRate, updateRate, deleteRate, CurrentOpsRate, currentTxByteRate, currentRxByteRate, currentAvgLatency)
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
			totalOps, currentTxByteRateStr, currentRxByteRateStr))
		w.Flush()
		prevSetupWriteCount = setupWriteCount
		prevWriteCount = writeCount
		prevReadCount = readCount
		prevReadCursorCount = readCursorCount
		prevUpdateCount = updateCount
		prevDeleteCount = deleteCount
		prevTotalLatency = totalLatency
		prevTxTotalBytes = txTotalBytes
		prevRxTotalBytes = rxTotalBytes
		prevTime = now
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

func (l *BenchmarkRunner) addRateMetricsDatapoints(now time.Time, setupWriteRate, writeRate, readRate, readCursorRate, updateRate, deleteRate, CurrentOpsRate, currentTxByteRate, currentRxByteRate, currentAvgLatency float64) {
	//pinsertRate := writeRate
	l.testResult.SetupWriteRateTs = append(l.testResult.SetupWriteRateTs, *NewDataPoint(now.Unix(), wrapNaN(setupWriteRate)))
	l.testResult.WriteRateTs = append(l.testResult.WriteRateTs, *NewDataPoint(now.Unix(), wrapNaN(writeRate)))
	l.testResult.ReadRateTs = append(l.testResult.ReadRateTs, *NewDataPoint(now.Unix(), wrapNaN(readRate)))
	l.testResult.ReadCursorRateTs = append(l.testResult.ReadCursorRateTs, *NewDataPoint(now.Unix(), wrapNaN(readCursorRate)))
	l.testResult.UpdateRateTs = append(l.testResult.UpdateRateTs, *NewDataPoint(now.Unix(), wrapNaN(updateRate)))
	l.testResult.DeleteRateTs = append(l.testResult.DeleteRateTs, *NewDataPoint(now.Unix(), wrapNaN(deleteRate)))
	l.testResult.OverallOpsRateTs = append(l.testResult.OverallOpsRateTs, *NewDataPoint(now.Unix(), wrapNaN(CurrentOpsRate)))
	l.testResult.OverallTxByteRateTs = append(l.testResult.OverallTxByteRateTs, *NewDataPoint(now.Unix(), wrapNaN(currentTxByteRate)))
	l.testResult.OverallRxByteRateTs = append(l.testResult.OverallRxByteRateTs, *NewDataPoint(now.Unix(), wrapNaN(currentRxByteRate)))
	l.testResult.OverallAverageLatencyTs = append(l.testResult.OverallAverageLatencyTs, *NewDataPoint(now.Unix(), wrapNaN(currentAvgLatency)))
}

func calculateRateMetrics(setupWriteCount, prevSetupWriteCount, writeCount, prevWriteCount, readCount, prevReadCount, readCursorCount, prevReadCursorCount, updateCount, prevUpdateCount, deleteCount, prevDeleteCount, totalLatency, prevTotalLatency, txTotalBytes, prevTxTotalBytes, rxTotalBytes, prevRxTotalBytes uint64, took time.Duration, sinceStart time.Duration) (setupWriteRate, writeRate, readRate, readCursorRate, updateRate, deleteRate, CurrentOpsRate, overallOpsRate, currentAvgLatency, overallAvgLatency, currentTxByteRate, overallTxByteRate, currentRxByteRate, overallRxByteRate float64) {
	setupWriteRate = float64(setupWriteCount-prevSetupWriteCount) / float64(took.Seconds())
	writeRate = float64(writeCount-prevWriteCount) / float64(took.Seconds())
	readRate = float64(readCount-prevReadCount) / float64(took.Seconds())
	readCursorRate = float64(readCursorCount-prevReadCursorCount) / float64(took.Seconds())
	updateRate = float64(updateCount-prevUpdateCount) / float64(took.Seconds())
	deleteRate = float64(deleteCount-prevDeleteCount) / float64(took.Seconds())

	currentCount := (setupWriteCount - prevSetupWriteCount) + (writeCount - prevWriteCount) + (readCount - prevReadCount) + (readCursorCount - prevReadCursorCount) + (updateCount - prevUpdateCount) + (deleteCount - prevDeleteCount)
	currentLatency := totalLatency - prevTotalLatency
	currentAvgLatency = float64(currentLatency) / float64(currentCount)
	currentTxByteRate = float64(txTotalBytes-prevTxTotalBytes) / float64(took.Seconds())
	currentRxByteRate = float64(rxTotalBytes-prevRxTotalBytes) / float64(took.Seconds())

	CurrentOpsRate = setupWriteRate + writeRate + readRate + readCursorRate + updateRate + deleteRate

	totalCount := setupWriteCount + writeCount + readCount + readCursorCount + updateCount + deleteCount
	overallOpsRate = float64(totalCount) / float64(sinceStart.Seconds())
	overallAvgLatency = float64(totalLatency) / float64(totalCount)
	overallTxByteRate = float64(txTotalBytes) / float64(took.Seconds())
	overallRxByteRate = float64(rxTotalBytes) / float64(took.Seconds())
	return
}
