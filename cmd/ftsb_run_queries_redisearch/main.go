// tsbs_run_queries_siridb speed tests SiriDB using requests from stdin or file
//

// This program has no knowledge of the internals of the endpoint.
package main

import (
	"flag"
	"fmt"
	"github.com/garyburd/redigo/redis"
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
	host        string
	index       string
	withCursor  bool
	showExplain bool

	// Global vars:
	runner *query.BenchmarkRunner
	client *redisearch.Client
)

// Parse args:
func init() {
	runner = query.NewBenchmarkRunner()

	flag.StringVar(&host, "host", "localhost:6379", "Redis host address and port")
	flag.StringVar(&index, "index", "idx1", "RediSearch index")
	flag.BoolVar(&withCursor, "with-cursor", false, "If the query is FT.AGGREGRATE wether to include the WITHCRUSOR argument and process all responses until cursor id = 0")

	flag.Parse()
	client = redisearch.NewClient(host, "ftsb-run-queries-redisearch")
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

func (p *Processor) ProcessQuery(q query.Query, isWarm bool) ([]*query.Stat, uint64, error) {
	var queryCount uint64 = 0
	// No need to run again for EXPLAIN
	if isWarm && p.opts.showExplain {
		return nil, 0, nil
	}
	tq := q.(*query.RediSearch)
	total := 0
	var took int64 = 0
	timedOut := false
	var err error = nil
	var res [][]string = nil
	var docs []redisearch.Document = nil

	var queries []*query.Stat = make([]*query.Stat, 0, 1)

	qry := string(tq.RedisQuery)

	t := strings.Split(qry, ",")
	if len(t) < 2 {
		log.Fatalf("The query has not the correct format %s", qry)
	}
	command := t[0]
	if p.opts.debug {
		fmt.Println(strings.Join(t, " "))
	}
	stat := query.GetStat()

	switch command {
	case "FT.AGGREGATE":
		queryNum := t[1]
		agg := redisearch.NewAggregateQuery()
		switch queryNum {
		case "0":
			//0) Perf * Filter Query (get all records).
			agg = agg.SetQuery(redisearch.NewQuery(fmt.Sprintf("*")))

		case "1":
			//1) One year period, Exact Number of contributions by day, ordered chronologically, for a given editor,
			// paging over results into pages of size 31 ( month ) and retrieving one random deterministic page

			agg = agg.SetQuery(redisearch.NewQuery(fmt.Sprintf("@CURRENT_REVISION_EDITOR_USERNAME:%s @CURRENT_REVISION_TIMESTAMP:[%s %s]", t[2], t[3], t[4]))).
				SetMax(365).
				Apply(*redisearch.NewProjection("@CURRENT_REVISION_TIMESTAMP - (@CURRENT_REVISION_TIMESTAMP % 86400)", "day")).
				GroupBy(*redisearch.NewGroupBy().AddFields("@day").
					Reduce(*redisearch.NewReducerAlias(redisearch.GroupByReducerCount, []string{"@ID"}, "num_contributions"))).
				SortBy([]redisearch.SortingKey{*redisearch.NewSortingKeyDir("@day", false)}).
				Apply(*redisearch.NewProjection("timefmt(@day)", "day"))

		case "2":
			//2) One month period, Exact Number of distinct editors contributions by hour, ordered chronologically
			agg = agg.SetQuery(redisearch.NewQuery(fmt.Sprintf("@CURRENT_REVISION_TIMESTAMP:[%s %s]", t[2], t[3]))).
				SetMax(720).
				Apply(*redisearch.NewProjection("@CURRENT_REVISION_TIMESTAMP - (@CURRENT_REVISION_TIMESTAMP % 3600)", "hour")).
				GroupBy(*redisearch.NewGroupBy().AddFields("@hour").
					Reduce(*redisearch.NewReducerAlias(redisearch.GroupByReducerCount, []string{"@CURRENT_REVISION_EDITOR_USERNAME"}, "num_distinct_editors"))).
				SortBy([]redisearch.SortingKey{*redisearch.NewSortingKeyDir("@hour", false)}).
				Apply(*redisearch.NewProjection("timefmt(@hour)", "hour"))

		case "3":
			//3) One month period, Approximate Number of distinct editors contributions by hour, ordered chronologically
			agg = agg.SetQuery(redisearch.NewQuery(fmt.Sprintf("@CURRENT_REVISION_TIMESTAMP:[%s %s]", t[2], t[3]))).
				SetMax(720).
				Apply(*redisearch.NewProjection("@CURRENT_REVISION_TIMESTAMP - (@CURRENT_REVISION_TIMESTAMP % 3600)", "hour")).
				GroupBy(*redisearch.NewGroupBy().AddFields("@hour").
					Reduce(*redisearch.NewReducerAlias(redisearch.GroupByReducerCountDistinctish, []string{"@CURRENT_REVISION_EDITOR_USERNAME"}, "num_distinct_editors"))).
				SortBy([]redisearch.SortingKey{*redisearch.NewSortingKeyDir("@hour", false)}).
				Apply(*redisearch.NewProjection("timefmt(@hour)", "hour"))

		case "4":
			//4) One day period, Approximate Number of contributions by 5minutes interval by editor username, ordered first chronologically and second alphabetically by Revision editor username
			agg = agg.SetQuery(redisearch.NewQuery(fmt.Sprintf("@CURRENT_REVISION_TIMESTAMP:[%s %s]", t[2], t[3]))).
				SetMax(288).
				Apply(*redisearch.NewProjection("@CURRENT_REVISION_TIMESTAMP - (@CURRENT_REVISION_TIMESTAMP % 300)", "fiveMinutes")).
				GroupBy(*redisearch.NewGroupBy().AddFields([]string{"@fiveMinutes", "@CURRENT_REVISION_EDITOR_USERNAME"}).
					Reduce(*redisearch.NewReducerAlias(redisearch.GroupByReducerCountDistinctish, []string{"@ID"}, "num_contributions"))).
				Filter("@CURRENT_REVISION_EDITOR_USERNAME !=\"\"").
				SortBy([]redisearch.SortingKey{*redisearch.NewSortingKeyDir("@fiveMinutes", true), *redisearch.NewSortingKeyDir("@CURRENT_REVISION_EDITOR_USERNAME", false)}).
				Apply(*redisearch.NewProjection("timefmt(@fiveMinutes)", "fiveMinutes"))

		case "5":
			//5) One month period, Approximate Top 10 Revision editor usernames
			agg = agg.SetQuery(redisearch.NewQuery(fmt.Sprintf("@CURRENT_REVISION_TIMESTAMP:[%s %s]", t[2], t[3]))).
				SetMax(10).
				GroupBy(*redisearch.NewGroupBy().AddFields("@CURRENT_REVISION_EDITOR_USERNAME").
					Reduce(*redisearch.NewReducerAlias(redisearch.GroupByReducerCountDistinctish, []string{"@ID"}, "num_contributions"))).
				Filter("@CURRENT_REVISION_EDITOR_USERNAME !=\"\"").
				SortBy([]redisearch.SortingKey{*redisearch.NewSortingKeyDir("@num_contributions", true)}).
				Limit(0, 10)

		case "6":
			//6) One month period, Approximate Top 10 Revision editor usernames by number of Revisions broken by namespace (TAG field).
			agg = agg.SetQuery(redisearch.NewQuery(fmt.Sprintf("@CURRENT_REVISION_TIMESTAMP:[%s %s]", t[2], t[3]))).
				SetMax(10).
				GroupBy(*redisearch.NewGroupBy().AddFields([]string{"@NAMESPACE", "@CURRENT_REVISION_EDITOR_USERNAME"}).
					Reduce(*redisearch.NewReducerAlias(redisearch.GroupByReducerCountDistinctish, []string{"@ID"}, "num_contributions"))).
				Filter("@CURRENT_REVISION_EDITOR_USERNAME !=\"\"").
				SortBy([]redisearch.SortingKey{*redisearch.NewSortingKeyDir("@NAMESPACE", true), *redisearch.NewSortingKeyDir("@num_contributions", true)}).
				Limit(0, 10)

		case "7":
			//7) One month period, Top 10 editor username by average revision content.
			agg = agg.SetQuery(redisearch.NewQuery(fmt.Sprintf("@CURRENT_REVISION_TIMESTAMP:[%s %s]", t[2], t[3]))).
				SetMax(10).
				GroupBy(*redisearch.NewGroupBy().AddFields([]string{"@NAMESPACE", "@CURRENT_REVISION_EDITOR_USERNAME"}).
					Reduce(*redisearch.NewReducerAlias(redisearch.GroupByReducerAvg, []string{"@CURRENT_REVISION_CONTENT_LENGTH"}, "avg_rcl"))).
				SortBy([]redisearch.SortingKey{*redisearch.NewSortingKeyDir("@avg_rcl", false)}).
				Limit(0, 10)

		case "8":
			//8) Approximate average number of contributions by year each editor makes
			agg = agg.SetQuery(redisearch.NewQuery(fmt.Sprintf("@CURRENT_REVISION_EDITOR_USERNAME:%s", t[2]))).
				SetMax(365).
				Apply(*redisearch.NewProjection("@CURRENT_REVISION_TIMESTAMP - (@CURRENT_REVISION_TIMESTAMP % 86400)", "day")).
				GroupBy(*redisearch.NewGroupBy().AddFields("@day").
					Reduce(*redisearch.NewReducerAlias(redisearch.GroupByReducerCount, []string{"@ID"}, "num_contributions"))).
				SortBy([]redisearch.SortingKey{*redisearch.NewSortingKeyDir("@day", false)}).
				Apply(*redisearch.NewProjection("timefmt(@day)", "day"))

		default:
			queryCount = 0
			log.Fatalf("FT.AGGREGATE queryNum (%s) query not supported yet.", queryNum)
		}
		queryCount = 1
		label := q.HumanLabelName()
		if withCursor == true {
			agg = agg.SetCursor(redisearch.NewCursor())
			label = []byte(fmt.Sprintf("FT.AGGREGATE 1st itt :: %s", q.HumanLabelName()))
		}
		start := time.Now()

		res, total, err = client.Aggregate(agg)
		took = time.Since(start).Microseconds()
		timedOut = p.handleResponseAggregate(err, timedOut, t, res, total, agg.AggregatePlan)
		stat.Init(label, took, uint64(total), timedOut, t[1])
		queries = append(queries, []*query.Stat{stat}...)
		if withCursor == true {
			label = []byte(fmt.Sprintf("FT.CURSOR 2nd and next itts :: %s", q.HumanLabelName()))
			for agg.CursorHasResults() {
				cursorStat := query.GetStat()
				start := time.Now()
				res, total, err = client.Aggregate(agg)
				took = time.Since(start).Microseconds()
				timedOut = p.handleResponseAggregate(err, timedOut, t, res, total, agg.AggregatePlan)
				cursorStat.Init(label, took, uint64(total), timedOut, t[1])
				queries = append(queries, []*query.Stat{cursorStat}...)
				queryCount++
			}
		}

	case "FT.SPELLCHECK":
		rediSearchQuery := redisearch.NewQuery(t[1])
		distance, err := strconv.Atoi(t[3])
		if err != nil {
			log.Fatalf("Error converting distance. Error message:|%s|\n", err)
		}
		rediSearchSpellCheckOptions := redisearch.NewSpellCheckOptions(distance)
		start := time.Now()
		suggs, total, err := client.SpellCheck(rediSearchQuery, rediSearchSpellCheckOptions)
		took = time.Since(start).Microseconds()
		timedOut = p.handleResponseSpellCheck(err, timedOut, t, suggs, total)
		queryCount = 1
		stat.Init(q.HumanLabelName(), took, uint64(total), timedOut, t[1])
		queries = append(queries, []*query.Stat{stat}...)

	case "FT.SEARCH":
		rediSearchQuery := redisearch.NewQuery(t[1])
		start := time.Now()
		docs, total, err = client.Search(rediSearchQuery)
		took = time.Since(start).Microseconds()
		timedOut = p.handleResponseDocs(err, timedOut, t, docs, total)
		queryCount = 1
		stat.Init(q.HumanLabelName(), took, uint64(total), timedOut, t[1])
		queries = append(queries, []*query.Stat{stat}...)

	default:
		queryCount = 0
		log.Fatalf("Command not supported yet. %s", command)
	}

	return queries, queryCount, nil
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
			log.Fatalf("Command (%s) failed:%v\n\tError message:%v\tString Error message:|%s|\n", t, suggs, err, err.Error())
		}
	} else {
		if p.opts.printResponse {
			fmt.Println("\nRESPONSE: ", total)
		}
	}
	return timedOut
}

func (p *Processor) handleResponseAggregate(err error, timedOut bool, t []string, aggs [][]string, total int, args redis.Args) bool {
	if err != nil {
		switch err.Error() {
		case "Command timed out":
			timedOut = true
			fmt.Fprintln(os.Stderr, "Command timed out. Used query: ", t)
		case "Query matches no results":
		default:
			log.Fatalf("Command failed:%v\tError message:%v\tString Error message:|%s|\n", args, err, err.Error())
		}
	} else {
		if p.opts.printResponse {
			fmt.Println(fmt.Sprintf("\nRESPONSE: \n\t#results %d\n\tAggregate: %s", total, aggs))
		}
	}
	return timedOut
}
