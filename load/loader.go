package load

import (
	"bufio"
	"code.cloudfoundry.org/bytefmt"
	"encoding/json"
	"flag"
	"fmt"
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
// needed to run an insert or load benchmark.
type Benchmark interface {
	// GetPointDecoder returns the PointDecoder to use for this Benchmark
	GetPointDecoder(br *bufio.Reader) PointDecoder

	// GetBatchFactory returns the BatchFactory to use for this Benchmark
	GetBatchFactory() BatchFactory

	// GetPointIndexer returns the PointIndexer to use for this Benchmark
	GetPointIndexer(maxPartitions uint) PointIndexer

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
	useHashes       bool
	doCreateDB      bool
	doAbortOnExist  bool
	reportingPeriod time.Duration
	fileName        string
	insertRate      float64
	updateRate      float64
	deleteRate      float64

	// non-flag fields
	br           *bufio.Reader
	insertCount  uint64
	updateCount  uint64
	deleteCount  uint64
	totalLatency uint64
	totalBytes   uint64
	rowCnt       uint64

	testResult TestResult
}

func (l *BenchmarkRunner) InsertRate() float64 {
	return l.insertRate
}

func (l *BenchmarkRunner) DeleteRate() float64 {
	return l.deleteRate
}

func (l *BenchmarkRunner) UpdateRate() float64 {
	return l.updateRate
}

var loader = &BenchmarkRunner{}

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
	flag.BoolVar(&loader.doLoad, "do-load", true, "Whether to write databuild. Set this flag to false to check input read speed.")
	flag.BoolVar(&loader.doCreateDB, "do-create-db", true, "Whether to create the database. Disable on all but one client if running on a multi client setup.")
	flag.BoolVar(&loader.doAbortOnExist, "do-abort-on-exist", false, "Whether to abort if a database with the given name already exists.")
	flag.DurationVar(&loader.reportingPeriod, "reporting-period", 1*time.Second, "Period to report write stats")
	flag.StringVar(&loader.fileName, "file", "", "File name to read databuild from")
	flag.BoolVar(&loader.useHashes, "use-hashes", false, "If set to true, it will use hashes to insert the documents.")
	flag.Float64Var(&loader.updateRate, "update-rate", 0, "Set the update rate ( between 0-1 ) for Documents being ingested")
	flag.Float64Var(&loader.deleteRate, "delete-rate", 0, "Set the delete rate ( between 0-1 ) for Documents being ingested")
	flag.StringVar(&loader.JsonOutFile, "json-out-file", "", "Name of json output file to output load results. If not set, will not print to json.")
	flag.StringVar(&loader.Metadata, "metadata-string", "", "Metadata string to add to json-out-file. If -json-out-file is not set, will not use this option.")
	return loader
}

// DatabaseName returns the value of the --db-name flag (name of the database to store databuild)
func (l *BenchmarkRunner) DatabaseName() string {
	return l.dbName
}

// RunBenchmark takes in a Benchmark b, a bufio.Reader br, and holders for number of metrics and rows
// and uses those to run the load benchmark
func (l *BenchmarkRunner) RunBenchmark(b Benchmark, workQueues uint) {
	l.br = l.GetBufferedReader()

	// Create required DB
	cleanupFn := l.useDBCreator(b.GetDBCreator())
	defer cleanupFn()

	channels := l.createChannels(workQueues)
	l.updateRequestedInsertUpdateDeleteRatios()
	// Launch all worker processes in background
	var wg sync.WaitGroup
	for i := 0; i < int(l.workers); i++ {
		wg.Add(1)
		go l.work(b, &wg, channels[i%len(channels)], i)
	}

	w := new(tabwriter.Writer)
	w.Init(os.Stderr, 20, 0, 1, ' ', tabwriter.AlignRight)
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
	l.summary(start, end)
}

func (l *BenchmarkRunner) updateRequestedInsertUpdateDeleteRatios() {
	l.insertRate = 1.0 - l.updateRate - l.deleteRate
	l.testResult.RequestedInsertRatio = l.insertRate
	l.testResult.RequestedUpdateRatio = l.updateRate
	l.testResult.RequestedDeleteRatio = l.deleteRate
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
	return scanWithIndexer(channels, l.batchSize, l.limit, l.br, b.GetPointDecoder(l.br), b.GetBatchFactory(), b.GetPointIndexer(uint(len(channels))))
}

// work is the processing function for each worker in the loader
func (l *BenchmarkRunner) work(b Benchmark, wg *sync.WaitGroup, c *duplexChannel, workerNum int) {

	// Prepare processor
	proc := b.GetProcessor()
	proc.Init(workerNum, l.doLoad)

	// Process batches coming from duplexChannel.toWorker queue
	// and send ACKs into duplexChannel.toScanner queue
	for b := range c.toWorker {
		metricCnt, rowCnt, updateCount, deleteCount, totalLatency, totalBytes := proc.ProcessBatch(b, l.doLoad, l.updateRate, l.deleteRate, l.useHashes)
		atomic.AddUint64(&l.insertCount, metricCnt)
		atomic.AddUint64(&l.updateCount, updateCount)
		atomic.AddUint64(&l.deleteCount, deleteCount)
		atomic.AddUint64(&l.totalLatency, totalLatency)
		atomic.AddUint64(&l.totalBytes, totalBytes)
		atomic.AddUint64(&l.rowCnt, rowCnt)
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
	insertCount := atomic.LoadUint64(&l.insertCount)
	updateCount := atomic.LoadUint64(&l.updateCount)
	deleteCount := atomic.LoadUint64(&l.deleteCount)
	totalOps := insertCount + updateCount + deleteCount
	totalLatency := atomic.LoadUint64(&l.totalLatency)
	totalBytes := atomic.LoadUint64(&l.totalBytes)
	insertRate, updateRate, deleteRate, _, overallOpsRate, _, overallAvgLatency, _, overallByteRate := calculateRateMetrics(insertCount, 0, took, updateCount, 0, deleteCount, 0, totalLatency, 0, totalBytes, 0, took)
	byteRateStr := bytefmt.ByteSize(uint64(overallByteRate))

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

	//TotalInserts
	l.testResult.TotalInserts = insertCount

	//TotalUpdates
	l.testResult.TotalUpdates = updateCount

	//TotalDeletes
	l.testResult.TotalDeletes = deleteCount

	//TotalLatency
	l.testResult.TotalLatency = totalLatency

	//TotalBytes
	l.testResult.TotalBytes = totalBytes

	/////////
	// Overall Ratios
	/////////

	//MeasuredInsertRatio
	l.testResult.MeasuredInsertRatio = float64(insertCount) / float64(totalOps)

	//MeasuredUpdateRatio
	l.testResult.MeasuredUpdateRatio = float64(updateCount) / float64(totalOps)

	//MeasuredDeleteRatio
	l.testResult.MeasuredDeleteRatio = float64(deleteCount) / float64(totalOps)

	/////////
	// Overall Rates
	/////////

	//OverallAvgIndexingRate
	l.testResult.OverallAvgOpsRate = overallOpsRate

	//OverallAvgInsertRate
	l.testResult.OverallAvgInsertRate = insertRate

	//OverallAvgUpdateRate
	l.testResult.OverallAvgUpdateRate = updateRate

	//OverallAvgDeleteRate
	l.testResult.OverallAvgDeleteRate = deleteRate

	//OverallAvgLatency
	l.testResult.OverallAvgLatency = overallAvgLatency

	//OverallAvgByteRate
	l.testResult.OverallAvgByteRate = overallByteRate
	l.testResult.OverallAvgByteRateHumanReadable = fmt.Sprintf("%sB/sec", byteRateStr)

	printFn("\nSummary:\n")
	printFn("Loaded %d Documents in %0.3fsec with %d workers\n", l.insertCount, took.Seconds(), l.workers)
	printFn("\tMean rate:\n\t - Total %0.2f ops/sec\n\t - Inserts %0.2f docs/sec\n\t - Updates %0.2f docs/sec\n\t - Deletes %0.2f docs/sec\n", overallOpsRate, insertRate, updateRate, deleteRate)
	printFn("\tOverall Avg Latency: %0.3f msec\n", overallAvgLatency)
	printFn("\tOverall Byte Rate: %sB/sec\n", byteRateStr)

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
	prevInsertCount := uint64(0)
	prevUpdateCount := uint64(0)
	prevDeleteCount := uint64(0)
	prevTotalLatency := uint64(0)
	prevTotalBytes := uint64(0)
	fmt.Fprint(w, "time\tinserts/se\tupdates/sec\tdeletes/sec\tcurrent ops/sec\tcurr avg lat ms\tdocs total\toverall ops/sec\toverall avg lat ms \t overall BW/s\n")
	w.Flush()
	for now := range time.NewTicker(period).C {
		insertCount := atomic.LoadUint64(&l.insertCount)
		updateCount := atomic.LoadUint64(&l.updateCount)
		deleteCount := atomic.LoadUint64(&l.deleteCount)
		totalLatency := atomic.LoadUint64(&l.totalLatency)
		totalBytes := atomic.LoadUint64(&l.totalBytes)
		sinceStart := now.Sub(start)
		took := now.Sub(prevTime)
		insertRate, updateRate, deleteRate, CurrentOpsRate, overallOpsRate, currentAvgLatency, overallAvgLatency, currentByteRate, _ := calculateRateMetrics(insertCount, prevInsertCount, took, updateCount, prevUpdateCount, deleteCount, prevDeleteCount, totalLatency, prevTotalLatency, totalBytes, prevTotalBytes, sinceStart)
		byteRateStr := bytefmt.ByteSize(uint64(currentByteRate))

		l.addRateMetricsDatapoints(now, insertRate, updateRate, deleteRate, CurrentOpsRate, currentByteRate, currentAvgLatency)
		fmt.Fprint(w, fmt.Sprintf("%d \t %.1f \t %.1f \t %.1f \t %.1f \t %.3f \t %d \t %.1f \t %.3f \t %sB/s\n",
			now.Unix(), insertRate, updateRate, deleteRate, CurrentOpsRate, currentAvgLatency, insertCount, overallOpsRate, overallAvgLatency, byteRateStr))
		w.Flush()
		prevInsertCount = insertCount
		prevUpdateCount = updateCount
		prevDeleteCount = deleteCount
		prevTotalLatency = totalLatency
		prevTotalBytes = totalBytes
		prevTime = now
	}
}

func (l *BenchmarkRunner) addRateMetricsDatapoints(now time.Time, insertRate float64, updateRate float64, deleteRate float64, CurrentOpsRate float64, currentByteRate float64, currentAvgLatency float64) {
	l.testResult.InsertRateTs = append(l.testResult.InsertRateTs, *NewDataPoint(now.Unix(), insertRate))
	l.testResult.UpdateRateTs = append(l.testResult.UpdateRateTs, *NewDataPoint(now.Unix(), updateRate))
	l.testResult.DeleteRateTs = append(l.testResult.DeleteRateTs, *NewDataPoint(now.Unix(), deleteRate))
	l.testResult.OverallIngestionRateTs = append(l.testResult.OverallIngestionRateTs, *NewDataPoint(now.Unix(), CurrentOpsRate))
	l.testResult.OverallByteRateTs = append(l.testResult.OverallByteRateTs, *NewDataPoint(now.Unix(), currentByteRate))
	l.testResult.OverallAverageLatencyTs = append(l.testResult.OverallAverageLatencyTs, *NewDataPoint(now.Unix(), currentAvgLatency))
}

func calculateRateMetrics(insertCount uint64, prevInsertCount uint64, took time.Duration, updateCount uint64, prevUpdateCount uint64, deleteCount uint64, prevDeleteCount uint64, totalLatency uint64, prevTotalLatency uint64, totalBytes uint64, prevTotalBytes uint64, sinceStart time.Duration) (float64, float64, float64, float64, float64, float64, float64, float64, float64) {
	insertRate := float64(insertCount-prevInsertCount) / float64(took.Seconds())
	updateRate := float64(updateCount-prevUpdateCount) / float64(took.Seconds())
	deleteRate := float64(deleteCount-prevDeleteCount) / float64(took.Seconds())
	currentCount := (insertCount - prevInsertCount) + (updateCount - prevUpdateCount) + (deleteCount - prevDeleteCount)
	currentLatency := totalLatency - prevTotalLatency
	curentByteRate := float64(totalBytes-prevTotalBytes) / float64(took.Seconds())
	CurrentOpsRate := insertRate + updateRate + deleteRate
	overallOpsRate := float64(insertCount+updateCount+deleteCount) / float64(sinceStart.Seconds())
	overallAvgLatency := float64(totalLatency) / float64(insertCount+updateCount+deleteCount)
	currentAvgLatency := float64(currentLatency) / float64(currentCount)
	overallByteRate := float64(totalBytes) / float64(took.Seconds())
	return insertRate, updateRate, deleteRate, CurrentOpsRate, overallOpsRate, currentAvgLatency, overallAvgLatency, curentByteRate, overallByteRate
}
