package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/RediSearch/ftsb/load"
	"github.com/RediSearch/redisearch-go/redisearch"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Program option vars:
var (
	host                    string
	pipeline                uint64
	replacePartial          bool
	replacePartialCondition string
	debug                   int
	syntheticsCardinality   uint64
	syntheticsNumberFields  uint64
	loader                  *load.BenchmarkRunner
	useCase                 string
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
	flag.BoolVar(&replacePartial, "replace-partial", false, "(only applicable with REPLACE (when update rate is higher than 0))")
	flag.StringVar(&replacePartialCondition, "replace-condition", "", "(Applicable only in conjunction with REPLACE and optionally PARTIAL)")
	flag.Uint64Var(&syntheticsCardinality, "synthetic-max-dataset-cardinality", 1024, "Max Field cardinality specific to the synthetics use cases (e.g., distinct tags in 'tag' fields).")
	flag.Uint64Var(&syntheticsNumberFields, "synthetic-fields", 10, "Number of fields per document specific to the synthetics use cases (starting at field1, field2, field3, etc...).")
	flag.StringVar(&useCase, "use-case", "enwiki-abstract", fmt.Sprintf("Use case to model. (choices: %s)", strings.Join(useCaseChoices, ", ")))

	flag.IntVar(&debug, "debug", 0, "Debug printing (choices: 0, 1, 2). (default 0)")
	flag.Parse()
}

type benchmark struct {
	dbc *dbCreator
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
	return &processor{b.dbc, nil, nil, nil, nil, nil, nil, nil, nil, []string{}, []string{}, []string{},}
}

func (b *benchmark) GetDBCreator() load.DBCreator {
	return b.dbc
}

type processor struct {
	dbc              *dbCreator
	rows             chan string
	insertsChan      chan uint64
	totalLatencyChan chan uint64
	updatesChan      chan uint64
	deletesChan      chan uint64
	totalBytesChan   chan uint64
	wg               *sync.WaitGroup
	client           *redisearch.Client
	insertedDocIds   []string
	updatedDocIds    []string
	deletedDocIds    []string
}

//, client* redisearch.Client,  pipelineSize int, documents []redisearch.Document
func rowToRSDocument(row string) (document *redisearch.Document) {
	if debug > 0 {
		fmt.Fprintln(os.Stderr, "converting row to rediSearch Document "+row)
	}
	fieldSizesStr := strings.Split(row, ",")
	// we need at least the id and score
	if len(fieldSizesStr) >= 2 {
		documentId := loader.DatabaseName() + "-" + fieldSizesStr[0]
		documentScore, _ := strconv.ParseFloat(fieldSizesStr[1], 64)
		doc := redisearch.NewDocument(documentId, float32(documentScore))

		for _, keyValuePair := range fieldSizesStr[2:] {
			pair := strings.Split(keyValuePair, "=")
			if len(pair) == 2 {
				if debug > 0 {
					fmt.Fprintln(os.Stderr, "On doc "+documentId+" adding field with NAME "+pair[0]+" and VALUE "+pair[1])
				}
				doc.Set(pair[0], pair[1])
			} else {
				if debug > 0 {
					fmt.Fprintf(os.Stderr, "On doc "+documentId+" len(pair)=%d", len(pair))
				}
				log.Fatalf("keyValuePair pair size != 2 . Got " + keyValuePair)
			}
		}
		if debug > 0 {
			fmt.Fprintln(os.Stderr, "Doc "+documentId)
		}
		return &doc
	}
	return document
}

func connectionProcessor(p *processor, pipeline uint64, updateRate float64, deleteRate float64, updatePartial bool, updateCondition string) {
	var documents = make([]redisearch.Document, 0)

	pipelinePos := uint64(0)
	insertCount := uint64(0)
	// using random between [0,1) to determine whether it is an delete,update, or insert
	// DELETE IF BETWEEN [0,deleteLimit)
	// UPDATE IF BETWEEN [deleteLimit,updateLimit)
	// INSERT IF BETWEEN [updateLimit,1)

	deleteUpperLimit := 0.0
	updateUpperLimit := deleteUpperLimit + updateRate

	updateOpts := redisearch.IndexingOptions{
		Language:         "",
		NoSave:           false,
		Replace:          true,
		Partial:          updatePartial,
		ReplaceCondition: updateCondition,
	}

	for row := range p.rows {
		doc := rowToRSDocument(row)
		if doc != nil {

			val := rand.Float64()
			////DELETE
			//if val < deleteUpperLimit && ((len(p.insertedDocIds) - len(p.deletedDocIds) ) > 0) {
			//	p.insertedDocIds = append(p.insertedDocIds, doc.Id)
			//	deleteCount++
			// UPDATE
			// only possible if we already have something to update
			if val >= deleteUpperLimit && val < updateUpperLimit && (len(p.insertedDocIds) > 0) {
				p.insertedDocIds = append(p.insertedDocIds, doc.Id)
				idToUdpdate := p.insertedDocIds[rand.Intn(len(p.insertedDocIds))]
				doc.Id = idToUdpdate
				// make sure we flush the pipeline prior than updating
				if pipelinePos > 0 {
					// Index the document. The API accepts multiple documents at a time
					start := time.Now()
					if err := p.client.Index(documents...); err != nil {
						log.Fatalf("failed: %s\n", err)
					}
					took := uint64(time.Since(start).Milliseconds())
					p.totalLatencyChan <- took
					p.insertsChan <- insertCount

					documents = make([]redisearch.Document, 0)
					pipelinePos = 0
					insertCount = 0
				}
				start := time.Now()
				if err := p.client.IndexOptions(updateOpts, *doc); err != nil {
					log.Fatalf("failed: %s\n", err)
				}
				took := uint64(time.Since(start).Milliseconds())
				p.totalLatencyChan <- took
				p.updatesChan <- 1

				// INSERT
			} else {
				documents = append(documents, *doc)
				p.insertedDocIds = append(p.insertedDocIds, doc.Id)
				insertCount++
				pipelinePos++
			}
			if pipelinePos%pipeline == 0 && len(documents) > 0 {
				// Index the document. The API accepts multiple documents at a time
				start := time.Now()
				if err := p.client.Index(documents...); err != nil {
					log.Fatalf("failed: %s\n", err)
				}
				took := uint64(time.Since(start).Milliseconds())
				p.totalLatencyChan <- took
				p.insertsChan <- insertCount

				documents = make([]redisearch.Document, 0)
				pipelinePos = 0
				insertCount = 0
			}
		}

	}
	if pipelinePos != 0 && len(documents) > 0 {
		// Index the document. The API accepts multiple documents at a time
		start := time.Now()
		if err := p.client.Index(documents...); err != nil {
			log.Fatalf("failed: %s\n", err)
		}
		took := uint64(time.Since(start).Milliseconds())
		p.totalLatencyChan <- took
		p.insertsChan <- insertCount

		documents = make([]redisearch.Document, 0)
		pipelinePos = 0
		insertCount = 0
	}
	p.wg.Done()
}

func (p *processor) Init(_ int, _ bool) {
	p.client = redisearch.NewClient(host, loader.DatabaseName())
}

// ProcessBatch reads eventsBatches which contain rows of data for FT.ADD redis command string
func (p *processor) ProcessBatch(b load.Batch, doLoad bool, updateRate, deleteRate float64) (uint64, uint64, uint64, uint64, uint64, uint64) {
	events := b.(*eventsBatch)
	rowCnt := uint64(len(events.rows))
	metricCnt := uint64(0)
	updateCount := uint64(0)
	deleteCount := uint64(0)
	totalLatency := uint64(0)
	totalBytes := uint64(0)
	if doLoad {
		buflen := rowCnt + 1

		p.insertsChan = make(chan uint64, buflen)
		p.updatesChan = make(chan uint64, buflen)
		p.deletesChan = make(chan uint64, buflen)
		p.totalLatencyChan = make(chan uint64, buflen)
		p.totalBytesChan = make(chan uint64, buflen)

		p.wg = &sync.WaitGroup{}
		p.rows = make(chan string, buflen)
		p.wg.Add(1)
		go connectionProcessor(p, pipeline, updateRate, deleteRate, replacePartial, replacePartialCondition)
		for _, row := range events.rows {
			p.rows <- row
		}
		close(p.rows)
		p.wg.Wait()
		close(p.insertsChan)
		close(p.updatesChan)
		close(p.deletesChan)
		close(p.totalLatencyChan)
		close(p.totalBytesChan)

		for val := range p.insertsChan {
			metricCnt += val
		}
		for val := range p.updatesChan {
			updateCount += val
		}
		for val := range p.deletesChan {
			deleteCount += val
		}
		for val := range p.totalLatencyChan {
			totalLatency += val
		}
		for val := range p.totalBytesChan {
			totalBytes += val
		}

	}
	events.rows = events.rows[:0]
	ePool.Put(events)
	return metricCnt, rowCnt, updateCount, deleteCount, totalLatency, totalBytes
}

func (p *processor) Close(_ bool) {
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
