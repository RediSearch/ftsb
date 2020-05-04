package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/RediSearch/ftsb/load"
	"github.com/RediSearch/redisearch-go/redisearch"
	//"github.com/mediocregopher/radix"
	"log"
	"strings"
)

// Program option vars:
var (
	host                    string
	pipeline                uint64
	noSave                  bool
	replacePartial          bool
	replacePartialCondition string
	debug                   int
	syntheticsCardinality   uint64
	syntheticsNumberFields  uint64
	loader                  *load.BenchmarkRunner
	useCase                 string
	useHmset                bool
	useFtadd                bool
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
	flag.Uint64Var(&pipeline, "pipeline", 10, "The pipeline's size")
	flag.BoolVar(&noSave, "no-save", false, "If set to true, we will not save the actual document in the database and only index it.")
	flag.BoolVar(&replacePartial, "replace-partial", false, "(only applicable with REPLACE (when update rate is higher than 0))")
	flag.StringVar(&replacePartialCondition, "replace-condition", "", "(Applicable only in conjunction with REPLACE and optionally PARTIAL)")
	flag.Uint64Var(&syntheticsCardinality, "synthetic-max-dataset-cardinality", 1024, "Max Field cardinality specific to the synthetics use cases (e.g., distinct tags in 'tag' fields).")
	flag.Uint64Var(&syntheticsNumberFields, "synthetic-fields", 10, "Number of fields per document specific to the synthetics use cases (starting at field1, field2, field3, etc...).")
	flag.StringVar(&useCase, "use-case", "enwiki-abstract", fmt.Sprintf("Use case to model. (choices: %s)", strings.Join(useCaseChoices, ", ")))
	flag.BoolVar(&useHmset, "use-hmset", false, "If set to true, it will use hmset command to insert the documents.")
	flag.BoolVar(&useFtadd, "use-ftadd", false, "If set to true, it will use ft.add to insert the documents.")
	flag.IntVar(&debug, "debug", 0, "Debug printing (choices: 0, 1, 2). (default 0)")
	flag.Parse()
}

type benchmark struct {
	dbc *dbCreator
}

func (b *benchmark) GetConfigurationParametersMap() map[string]interface{} {
	configs := map[string]interface{}{}
	configs["host"] = host
	configs["pipeline"] = pipeline
	configs["replacePartial"] = replacePartial
	configs["replacePartialCondition"] = replacePartialCondition
	configs["syntheticsCardinality"] = syntheticsCardinality
	configs["syntheticsNumberFields"] = syntheticsNumberFields
	configs["useCase"] = useCase
	configs["debug"] = debug
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
	return &processor{b.dbc, nil, nil, nil, nil, nil, nil, nil, nil, []string{}, []string{}, []string{}, nil}
}

func (b *benchmark) GetDBCreator() load.DBCreator {
	return b.dbc
}

func LocalCountersReset() (documents []redisearch.Document, pipelinePos uint64, insertCount uint64, totalBytes uint64) {
	documents = make([]redisearch.Document, 0)
	pipelinePos = 0
	insertCount = 0
	totalBytes = 0
	return documents, insertCount, pipelinePos, totalBytes
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
	loader.RunBenchmark(&benchmark{dbc: &creator}, load.SingleQueue)
}
