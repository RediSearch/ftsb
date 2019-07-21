package redisearch

import (
	"fmt"
	"github.com/filipecosta90/ftsb/query"
	"github.com/filipecosta90/ftsb/cmd/ftsb_generate_queries/uses/wiki"
	"time"
)

// EnWikiAbstract produces RediSearch-specific queries for all the devops query types.
type EnWikiAbstract struct {
	*wiki.Core
}

// NewEnWikiAbstract makes an EnWikiAbstract object ready to generate Queries.
func NewEnWikiAbstract( filename string ) *EnWikiAbstract {
	return &EnWikiAbstract{wiki.NewCore( filename )}
}

// GenerateEmptyQuery returns an empty query.RediSearch
func (d *EnWikiAbstract) GenerateEmptyQuery() query.Query {
	return query.NewRediSearch()
}

// Simple2WordQuery fetches the MAX for numMetrics metrics under 'cpu', per minute for nhosts hosts,
// every 1 mins for 1 hour
func (d *EnWikiAbstract) Simple2WordQuery(qi query.Query, nHosts, numMetrics int, timeRange time.Duration) {

	redisQuery := fmt.Sprintf(`FT.SEARCH,%s,\"%s %s\"'`,
		"idx1",
		"barack",
		"obama")
	humanLabel := fmt.Sprintf("RediSearch FT.SEARCH %s \"%s %s\"", "idx1",
		"barack",
		"obama")
	humanDesc := fmt.Sprintf("%s", humanLabel)
	d.fillInQuery(qi, humanLabel, humanDesc, redisQuery)

}


// fill Query fills the query struct with data
func (d *EnWikiAbstract) fillInQuery(qi query.Query, humanLabel, humanDesc, redisQuery string) {
	q := qi.(*query.RediSearch)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
	q.RedisQuery = []byte(redisQuery)
}
