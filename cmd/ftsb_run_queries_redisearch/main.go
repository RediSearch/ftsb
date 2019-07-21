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
	"github.com/gomodule/redigo/redis"
	_ "github.com/lib/pq"
)

// Program option vars:
var (
	host        string
	showExplain bool
	//	scale        uint64
)

// Global vars:
var (
	runner *query.BenchmarkRunner
)

var (
	pool *redis.Pool
)

// Parse args:
func init() {
	runner = query.NewBenchmarkRunner()

	flag.StringVar(&host, "host", "localhost:6379", "Redis host address and port")
	//flag.Uint64Var(&scale, "scale", 8, "Scaling variable (Must be the equal to the scalevar used for data generation).")

	flag.Parse()

	pool = &redis.Pool{
		MaxIdle:     50,
		IdleTimeout: 60 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", host)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		MaxActive: 1000,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {

			_, err := c.Do("PING")
			if err != nil {
				log.Printf("[ERROR]: TestOnBorrow failed healthcheck to redisUrl=%q err=%v",
					host, err)
			}
			return err
		},
		Wait: true, // pool.Get() will block waiting for a free connection
	}
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
	conn := pool.Get()
	t := strings.Split(qry, ",")
	commandArgs := make([]interface{}, len(t)-1)
	for i := 1; i < len(t); i++ {
		commandArgs[i-1] = t[i]
	}
	start := time.Now()
	res, err := conn.Do(t[0], commandArgs...)
	if err != nil {
		log.Fatalf("Command failed:%v %v\n", res, err)
	}

	if p.opts.debug {
		fmt.Println(qry)
	}

	if p.opts.printResponse {
		fmt.Println("\n", res)
	}

	took := float64(time.Since(start).Nanoseconds()) / 1e6
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), took)

	return []*query.Stat{stat}, err
}
