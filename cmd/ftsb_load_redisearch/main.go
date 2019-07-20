package main

import (
	"bufio"
	"flag"
	"github.com/filipecosta90/ftsb/load"
	"github.com/gomodule/redigo/redis"
	"log"
	"strconv"
	"strings"
	"sync"
)

// Program option vars:
var (
	host        string
	connections uint64
	pipeline    uint64
	checkChunks uint64
	singleQueue bool
	dataModel   string
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
	flag.Uint64Var(&connections, "connections", 10, "The number of connections per worker")
	flag.Uint64Var(&pipeline, "pipeline", 50, "The pipeline's size")
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
	return &processor{b.dbc, nil, nil, nil}
}

func (b *benchmark) GetDBCreator() load.DBCreator {
	return b.dbc
}

type processor struct {
	dbc     *dbCreator
	rows    []chan string
	metrics chan uint64
	wg      *sync.WaitGroup
}

func connectionProcessor(wg *sync.WaitGroup, rows chan string, metrics chan uint64, conn redis.Conn, id uint64) {
	//curPipe := uint64(0)
	for row := range rows {

		nFieldsStr := strings.SplitN( row, ",", 2 )
		if len(nFieldsStr)!=2{
			log.Fatalf("row does not have the correct format( len %d ) %s failed\n",  len(nFieldsStr) , row )
		}
		nFields, _ := strconv.Atoi(nFieldsStr[0])

		fieldSizesStr := strings.SplitN(nFieldsStr[1],",", nFields+1)
		ftsRow := fieldSizesStr[nFields]
		var cmdArgs []string

		previousPos := 0
		fieldLen := 0
		for i := 0; i < nFields; i++ {
			fieldLen, _ = strconv.Atoi(fieldSizesStr[i])
			cmdArgs = append(cmdArgs, ftsRow[previousPos:(previousPos + fieldLen)])
			previousPos = previousPos + fieldLen

		}

		s := redis.Args{}.AddFlat(cmdArgs)
		metricValue := uint64(1)

		_, err := conn.Do("FT.ADD", s...)
		////err := conn.Send(t[0], s...)
		if err != nil {
			log.Fatalf("FT.ADD %s failed: %s\n", s, err)
			metricValue = uint64(0)
		}

		//sendRedisCommand(row, conn)
		metrics <- metricValue
		//curPipe++
	}
	//if curPipe > 0 {
	//	cnt, err := sendRedisFlush(curPipe, conn)
	//	if err != nil {
	//		log.Fatalf("Flush failed with %v", err)
	//	}
	//	metrics <- cnt
	//}
	wg.Done()
}

func (p *processor) Init(_ int, _ bool) {}

// ProcessBatch reads eventsBatches which contain rows of data for TS.ADD redis command string
func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (uint64, uint64) {
	events := b.(*eventsBatch)
	rowCnt := uint64(len(events.rows))
	metricCnt := uint64(0)
	// indexer := &RedisIndexer{partitions: uint(connections)}
	if doLoad {
		buflen := rowCnt + 1
		p.rows = make([]chan string, connections)
		p.metrics = make(chan uint64, buflen)
		p.wg = &sync.WaitGroup{}
		for i := uint64(0); i < connections; i++ {
			conn := p.dbc.pool.Get()
			defer conn.Close()
			p.rows[i] = make(chan string, buflen)
			p.wg.Add(1)
			go connectionProcessor(p.wg, p.rows[i], p.metrics, conn, i)
		}
		pos :=uint64(0)
		for _, row := range events.rows {
			i := pos % connections
			p.rows[i] <- row
			pos++
		}

		for i := uint64(0); i < connections; i++ {
			close(p.rows[i])
		}
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
	workQueues := uint(load.WorkerPerQueue)
	loader.RunBenchmark(&benchmark{dbc: &dbCreator{}}, workQueues)
}
