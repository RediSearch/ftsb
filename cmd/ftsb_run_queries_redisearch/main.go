// tsbs_run_queries_siridb speed tests SiriDB using requests from stdin or file
//

// This program has no knowledge of the internals of the endpoint.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/RediSearch/ftsb/query"
	"github.com/RediSearch/redisearch-go/redisearch"
	_ "github.com/lib/pq"
)

// Program option vars:
var (
	host  string
	index string

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
	client = redisearch.NewClient(host, index)
}

func main() {
	runner.Run(&query.RediSearchPool, newProcessor)
}

type queryExecutorOptions struct {
	showExplain   bool
	debug         bool
	printResponse bool
}

type Processor struct {
	opts          *queryExecutorOptions
	Metrics       chan uint64
	ResponseSizes chan uint64
	Wg            *sync.WaitGroup
}

func newProcessor() query.Processor { return &Processor{} }

func (p *Processor) Init(numWorker int, wg *sync.WaitGroup, m chan uint64, rs chan uint64) {
	p.Wg = wg
	p.Metrics = m
	p.ResponseSizes = rs

	p.opts = &queryExecutorOptions{
		showExplain:   showExplain,
		debug:         runner.DebugLevel() > 0,
		printResponse: runner.DoPrintResponses(),
	}
}

func (p *Processor) ProcessQuery(q query.Query, isWarm bool) ([]*query.Stat, error) {

	// No need to run again for EXPLAIN
	if isWarm && p.opts.showExplain {
		return nil, nil
	}
	tq := q.(*query.RediSearch)
	total := 0
	took := 0.0
	timedOut := false

	qry := string(tq.RedisQuery)

	t := strings.Split(qry, ",")
	if len(t) < 2 {
		log.Fatalf("The query has not the correct format ", qry)
	}
	command := t[0]
	if p.opts.debug {
		fmt.Println(strings.Join(t, " "))
	}

	switch command {
	case "FT.AGGREGATE":
		queryNum := t[1]
		query := redisearch.NewAggregateQuery()
		switch queryNum {
		case "1":
			//1) One year period, Exact Number of contributions by day, ordered chronologically
			query.SetQuery(redisearch.NewQuery(t[2])).
				SetMax(365).
				Apply(*redisearch.NewProjection("@CURRENT_REVISION_TIMESTAMP - (@CURRENT_REVISION_TIMESTAMP % 86400)", "day")).
				GroupBy(*redisearch.NewGroupBy("@day").
					Reduce(*redisearch.NewReducerAlias(redisearch.GroupByReducerCount, []string{"@ID"}, "num_contributions"))).
				SortBy([]redisearch.SortingKey{*redisearch.NewSortingKeyDir("@day", false)}).
				Apply(*redisearch.NewProjection("timefmt(@day)", "day"))

		default:
			log.Fatalf("FT.AGGREGATE queryNum (%d) query not supported yet.", queryNum)
		}

		start := time.Now()
		res, total, err := client.Aggregate(query)
		took = float64(time.Since(start).Nanoseconds()) / 1e6
		timedOut = p.handleResponseAggregate(err, timedOut, t, res, total)

	case "FT.SPELLCHECK":
		rediSearchQuery := redisearch.NewQuery(t[1])
		distance, err := strconv.Atoi(t[2])
		if err != nil {
			log.Fatalf("Error converting distance. Error message:|%s|\n", err)
		}
		rediSearchSpellCheckOptions := redisearch.NewSpellCheckOptions(distance)
		start := time.Now()
		suggs, total, err := client.SpellCheck(rediSearchQuery, rediSearchSpellCheckOptions)
		took = float64(time.Since(start).Nanoseconds()) / 1e6
		timedOut = p.handleResponseSpellCheck(err, timedOut, t, suggs, total)

	case "FT.SEARCH":
		rediSearchQuery := redisearch.NewQuery(t[1])
		start := time.Now()
		docs, total, err := client.Search(rediSearchQuery)
		took = float64(time.Since(start).Nanoseconds()) / 1e6
		timedOut = p.handleResponseDocs(err, timedOut, t, docs, total)

	default:
		log.Fatalf("Command not supported yet.", command)
	}

	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), took, uint64(total), timedOut, t[1])

	return []*query.Stat{stat}, nil
}

func (p *Processor) handleResponseDocs(err error, timedOut bool, t []string, docs []redisearch.Document, total int) bool {
	if err != nil {
		if err.Error() == "Command timed out" {
			timedOut = true
			fmt.Fprintln(os.Stderr, "Command timed out. Used query: ", t)
		} else {
			log.Fatalf("Command failed:%v\tError message:%v\tString Error message:|%s|\n", docs, err, err.Error())
		}
	} else {
		if p.opts.printResponse {
			fmt.Println("\nRESPONSE: ", total)
		}
	}
	return timedOut
}

func (p *Processor) handleResponseSpellCheck(err error, timedOut bool, t []string, suggs []redisearch.MisspelledTerm, total int) bool {
	if err != nil {
		if err.Error() == "Command timed out" {
			timedOut = true
			fmt.Fprintln(os.Stderr, "Command timed out. Used query: ", t)
		} else {
			log.Fatalf("Command failed:%v\tError message:%v\tString Error message:|%s|\n", suggs, err, err.Error())
		}
	} else {
		if p.opts.printResponse {
			fmt.Println("\nRESPONSE: ", total)
		}
	}
	return timedOut
}

func (p *Processor) handleResponseAggregate(err error, timedOut bool, t []string, aggs [][]string, total int) bool {
	if err != nil {
		if err.Error() == "Command timed out" {
			timedOut = true
			fmt.Fprintln(os.Stderr, "Command timed out. Used query: ", t)
		} else {
			log.Fatalf("Command failed:%v\tError message:%v\tString Error message:|%s|\n", aggs, err, err.Error())
		}
	} else {
		if p.opts.printResponse {
			fmt.Println("\nRESPONSE: ", total)
		}
	}
	return timedOut
}
