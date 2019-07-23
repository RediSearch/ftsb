// tsbs_run_queries_siridb speed tests SiriDB using requests from stdin or file
//

// This program has no knowledge of the internals of the endpoint.
package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/filipecosta90/ftsb/query"
	"github.com/RediSearch/redisearch-go/redisearch"
	_ "github.com/lib/pq"
)

// Program option vars:
var (
	host        string
	index        string

	showExplain bool
	//	scale        uint64
)

// Global vars:
var (
	runner *query.BenchmarkRunner
)

var (
	client *redisearch.Client

)

// Parse args:
func init() {
	runner = query.NewBenchmarkRunner()

	flag.StringVar(&host, "host", "localhost:6379", "Redis host address and port")
	flag.StringVar(&index, "index", "idx1", "RediSearch index")
	flag.Parse()
	client = redisearch.NewClient(host,index)
}

func main() {
	runner.Run(&query.RediSearchPool, newProcessor)
}

type queryExecutorOptions struct {
	showExplain   bool
	debug         bool
	printResponse bool
}

type processor struct {
	opts *queryExecutorOptions
}

func newProcessor() query.Processor { return &processor{} }

func (p *processor) Init(numWorker int) {
	p.opts = &queryExecutorOptions{
		showExplain:   showExplain,
		debug:         runner.DebugLevel() > 0,
		printResponse: runner.DoPrintResponses(),
	}
}

func (p *processor) ProcessQuery(q query.Query, isWarm bool) ([]*query.Stat, error) {

	// No need to run again for EXPLAIN
	if isWarm && p.opts.showExplain {
		return nil, nil
	}
	tq := q.(*query.RediSearch)

	qry := string(tq.RedisQuery)

	t := strings.Split(qry, ",")
	if len(t) < 2 {
		log.Fatalf("The query has not the correct format ", qry )
	}
	command := t[0]
	if command != "FT.SEARCH" {
		log.Fatalf("Command not supported yet. Only FT.SEARCH. ", command )
	}
	rediSearchQuery := redisearch.NewQuery(t[1])
	start := time.Now()
	docs, total, err := client.Search(rediSearchQuery)
	took := float64(time.Since(start).Nanoseconds()) / 1e6

	if p.opts.debug {
		fmt.Println(strings.Join(t, " "))
	}
	//err := nil
	if err != nil {
		log.Fatalf("Command failed:%v|\t%v\n", docs, err)
	} else {
		if p.opts.printResponse {
			fmt.Println("\nRESPONSE: ", total)
		}
	}

	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), took, int64(total))

	return []*query.Stat{stat}, nil
}
