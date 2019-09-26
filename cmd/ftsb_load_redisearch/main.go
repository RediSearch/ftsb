package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/RediSearch/ftsb/load"
	"github.com/RediSearch/redisearch-go/redisearch"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

// Program option vars:
var (
	host     string
	index    string
	pipeline uint64
	debug    int
)

// Global vars
var (
	loader *load.BenchmarkRunner
	//bufPool sync.Pool
)

// allows for testing
var fatal = log.Fatal

// Parse args:
func init() {
	loader = load.GetBenchmarkRunnerWithBatchSize(1000)
	flag.StringVar(&host, "host", "localhost:6379", "The host:port for Redis connection")
	flag.Uint64Var(&pipeline, "pipeline", 10, "The pipeline's size")
	flag.StringVar(&index, "index", "idx1", "RediSearch index")
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
	return &processor{b.dbc, nil, nil, nil, nil}
}

func (b *benchmark) GetDBCreator() load.DBCreator {
	return b.dbc
}

type processor struct {
	dbc     *dbCreator
	rows    chan string
	metrics chan uint64
	wg      *sync.WaitGroup
	client  *redisearch.Client
}

//, client* redisearch.Client,  pipelineSize int, documents []redisearch.Document
func rowToRSDocument(row string) (document redisearch.Document) {
	if debug > 0 {
		fmt.Fprintln(os.Stderr, "converting row to rediSearch Document "+row)
	}
	nFieldsStr := strings.SplitN(row, ",", 2)
	if len(nFieldsStr) != 2 {
		log.Fatalf("row does not have the correct format( len %d ) %s failed\n", len(nFieldsStr), row)
	}
	nFields, _ := strconv.Atoi(nFieldsStr[0])

	if debug > 0 {
		fmt.Fprintln(os.Stderr, "Document has "+nFieldsStr[0]+"fields")
	}

	fieldSizesStr := strings.SplitN(nFieldsStr[1], ",", nFields+1)
	ftsRow := fieldSizesStr[nFields]
	previousPos := 0
	fieldLen := 0
	fieldLen, _ = strconv.Atoi(fieldSizesStr[0])
	documentId := index + "-" + ftsRow[previousPos:(previousPos+fieldLen)]
	previousPos = previousPos + fieldLen
	fieldLen, _ = strconv.Atoi(fieldSizesStr[1])
	documentScore, _ := strconv.ParseFloat(ftsRow[previousPos:(previousPos+fieldLen)], 64)
	previousPos = previousPos + fieldLen
	if debug > 0 {
		fmt.Fprintln(os.Stderr, "Doc "+documentId)
	}

	doc := redisearch.NewDocument(documentId, float32(documentScore))

	for i := 2; i < nFields; i = i + 2 {
		fieldLen, _ = strconv.Atoi(fieldSizesStr[i])
		fieldName := ftsRow[previousPos:(previousPos + fieldLen)]
		previousPos = previousPos + fieldLen
		fieldLen, _ = strconv.Atoi(fieldSizesStr[i+1])
		fieldValue := ftsRow[previousPos:(previousPos + fieldLen)]
		previousPos = previousPos + fieldLen
		if debug > 0 {
			fmt.Fprintln(os.Stderr, "On doc "+documentId+" adding field with NAME "+fieldName+" and VALUE "+fieldValue)
		}
		doc.Set(fieldName, fieldValue)
	}
	return doc
}

func connectionProcessor(wg *sync.WaitGroup, rows chan string, metrics chan uint64, client *redisearch.Client, pipeline uint64) {
	var documents []redisearch.Document = make([]redisearch.Document, 0)

	pipelinePos := uint64(0)
	for row := range rows {
		doc := rowToRSDocument(row)
		documents = append(documents, doc)
		pipelinePos++
		if pipelinePos%pipeline == 0 {
			// Index the document. The API accepts multiple documents at a time
			if err := client.Index(documents...); err != nil {
				log.Fatalf("failed: %s\n", err)
			}
			metrics <- pipelinePos
			documents = make([]redisearch.Document, 0)
			pipelinePos = 0
		}

	}
	if pipelinePos != 0 {
		// Index the document. The API accepts multiple documents at a time
		if err := client.Index(documents...); err != nil {
			log.Fatalf("failed: %s\n", err)
		}
		metrics <- pipelinePos
		documents = make([]redisearch.Document, 0)
		pipelinePos = 0
	}
	wg.Done()
}

func (p *processor) Init(_ int, _ bool) {
	p.client = redisearch.NewClient(host, index)
}

// ProcessBatch reads eventsBatches which contain rows of data for FT.ADD redis command string
func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (uint64, uint64) {
	events := b.(*eventsBatch)
	rowCnt := uint64(len(events.rows))
	metricCnt := uint64(0)
	if doLoad {
		buflen := rowCnt + 1
		p.metrics = make(chan uint64, buflen)
		p.wg = &sync.WaitGroup{}
		p.rows = make(chan string, buflen)
		p.wg.Add(1)
		go connectionProcessor(p.wg, p.rows, p.metrics, p.client, pipeline)
		for _, row := range events.rows {
			p.rows <- row
		}
		close(p.rows)
		p.wg.Wait()
		close(p.metrics)

		for val := range p.metrics {
			metricCnt += val
		}
	}
	events.rows = events.rows[:0]
	ePool.Put(events)
	return metricCnt, rowCnt
}

func (p *processor) Close(_ bool) {
}

func main() {
	//workQueues := uint(load.WorkerPerQueue)
	loader.RunBenchmark(&benchmark{dbc: &dbCreator{}}, load.SingleQueue)
}
