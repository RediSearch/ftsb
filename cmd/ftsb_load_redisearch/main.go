package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/RediSearch/ftsb/load"
	//"github.com/RediSearch/redisearch-go/redisearch"
	"time"

	//"github.com/mediocregopher/radix"
	"log"
	"strings"
)

// Program option vars:
var (
	host                    string
	noSave                  bool
	replacePartial          bool
	replacePartialCondition string
	debug                   int
	syntheticsCardinality   uint64
	syntheticsNumberFields  uint64
	loader                  *load.BenchmarkRunner
	useCase                 string
	isSynthethics           bool
	PoolPipelineConcurrency int
	PoolPipelineWindow      time.Duration
	useHashes               bool
	clusterMode             bool
)

const (
	// Use case choices (make sure to update TestGetConfig if adding a new one)
	useCaseEnWikiAbstract = "enwiki-abstract"
	// Use case choices (make sure to update TestGetConfig if adding a new one)
	useCaseEnWikiPages = "enwiki-pages"
	// Use case choices (make sure to update TestGetConfig if adding a new one)
	useCaseEcommerce = "ecommerce-electronic"
	// Use case choices (make sure to update TestGetConfig if adding a new one)
	useCaseSyntheticTags = "synthetic-tag"
	// Use case choices (make sure to update TestGetConfig if adding a new one)
	useCaseSyntheticText = "synthetic-text"
	// Use case choices (make sure to update TestGetConfig if adding a new one)
	useCaseSyntheticNumericInt = "synthetic-numeric-int"
	// Use case choices (make sure to update TestGetConfig if adding a new one)
	useCaseSyntheticNumericDouble = "synthetic-numeric-double"
)

// semi-constants
var (
	useCaseChoices = []string{
		useCaseEnWikiAbstract,
		useCaseEnWikiPages,
		useCaseEcommerce,
		useCaseSyntheticTags,
		useCaseSyntheticText,
		useCaseSyntheticNumericInt,
		useCaseSyntheticNumericDouble,
	}
	// allows for testing
	fatal = log.Fatalf
)

// Parse args:
func init() {
	loader = load.GetBenchmarkRunnerWithBatchSize(1000)
	flag.StringVar(&host, "host", "localhost:6379", "The host:port for Redis connection")
	flag.BoolVar(&noSave, "no-save", false, "If set to true, we will not save the actual document in the database and only index it.")
	flag.BoolVar(&replacePartial, "replace-partial", false, "(only applicable with REPLACE (when update rate is higher than 0))")
	flag.StringVar(&replacePartialCondition, "replace-condition", "", "(Applicable only in conjunction with REPLACE and optionally PARTIAL)")
	flag.Uint64Var(&syntheticsCardinality, "synthetic-max-dataset-cardinality", 1024, "Max Field cardinality specific to the synthetics use cases (e.g., distinct tags in 'tag' fields).")
	flag.Uint64Var(&syntheticsNumberFields, "synthetic-fields", 10, "Number of fields per document specific to the synthetics use cases (starting at field1, field2, field3, etc...).")
	flag.StringVar(&useCase, "use-case", "enwiki-abstract", fmt.Sprintf("Use case to model. (choices: %s)", strings.Join(useCaseChoices, ", ")))
	flag.IntVar(&debug, "debug", 0, "Debug printing (choices: 0, 1, 2). (default 0)")
	flag.BoolVar(&useHashes, "use-hashes", false, "If set to true, it will use hashes to insert the documents.")
	flag.BoolVar(&clusterMode, "cluster-mode", false, "If set to true, it will run the client in cluster mode.")
	flag.DurationVar(&PoolPipelineWindow, "pipeline-window", 500*time.Microsecond, "If window is zero then implicit pipelining will be disabled")
	flag.IntVar(&PoolPipelineConcurrency, "pipeline-max-size", 100, "If limit is zero then no limit will be used and pipelines will only be limited by the specified time window")

	flag.Parse()
	if (useCase == useCaseSyntheticText) ||
		(useCase == useCaseSyntheticNumericInt) ||
		(useCase == useCaseSyntheticNumericDouble) ||
		(useCase == useCaseSyntheticTags) {
		isSynthethics = true
	}
}

type benchmark struct {
	dbc *dbCreator
}

func (b *benchmark) GetConfigurationParametersMap() map[string]interface{} {
	configs := map[string]interface{}{}
	configs["host"] = host
	configs["replacePartial"] = replacePartial
	configs["replacePartialCondition"] = replacePartialCondition
	configs["syntheticsCardinality"] = syntheticsCardinality
	configs["syntheticsNumberFields"] = syntheticsNumberFields
	configs["useCase"] = useCase
	configs["useHashes"] = useHashes
	configs["clusterMode"] = clusterMode
	configs["debug"] = debug
	configs["isSynthethics"] = isSynthethics
	configs["PoolPipelineWindow"] = PoolPipelineWindow
	configs["PoolPipelineConcurrency"] = PoolPipelineConcurrency
	return configs
}

type RedisIndexer struct {
	partitions uint
}

func (i *RedisIndexer) GetIndex(itemsRead uint64, p *load.Point) int {
	return int(uint(itemsRead) % i.partitions)
}

func (b *benchmark) GetPointDecoder(br *bufio.Reader) load.PointDecoder {
	return &decoder{scanner: bufio.NewScanner(br)}
}

func (b *benchmark) GetBatchFactory() load.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(maxPartitions uint) load.PointIndexer {
	return &RedisIndexer{partitions: maxPartitions}
}

func (b *benchmark) GetProcessor() load.Processor {
	return &processor{b.dbc, nil, nil, nil, nil, nil, nil, nil, nil, []string{}, []string{}, []string{}, nil, nil}
}

func (b *benchmark) GetDBCreator() load.DBCreator {
	return b.dbc
}

func main() {
	//workQueues := uint(load.WorkerPerQueue)
	var isSynthethics bool
	if (useCase == useCaseSyntheticText) ||
		(useCase == useCaseSyntheticNumericInt) ||
		(useCase == useCaseSyntheticNumericDouble) ||
		(useCase == useCaseSyntheticTags) {
		isSynthethics = true
	}
	creator := dbCreator{
		nil, nil,
		syntheticsCardinality,
		syntheticsNumberFields,
		isSynthethics,
		useCase,
	}
	loader.RunBenchmark(&benchmark{dbc: &creator}, load.WorkerPerQueue)
}
