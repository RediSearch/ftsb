package redisearch

import (
	"fmt"
	"github.com/filipecosta90/ftsb/query"
	"github.com/filipecosta90/ftsb/cmd/ftsb_generate_queries/uses/wiki"
	"time"
)

// EnWikiAbstract produces RediSearch-specific queries for all the devops query types.
type EnWikiAbstract struct {
	Core *wiki.Core
}

// NewEnWikiAbstract makes an EnWikiAbstract object ready to generate Queries.
func NewEnWikiAbstract( filename string, seed int64, maxQueries int ) *EnWikiAbstract {
	return &EnWikiAbstract{wiki.NewCore( filename, seed, maxQueries )}
}

// GenerateEmptyQuery returns an empty query.RediSearch
func (d *EnWikiAbstract) GenerateEmptyQuery() query.Query {
	return query.NewRediSearch()
}

// Simple2WordQuery fetches the MAX for numMetrics metrics under 'cpu', per minute for nhosts hosts,
// every 1 mins for 1 hour
func (d *EnWikiAbstract) Simple2WordQuery(qi query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	if d.Core.QueryIndexPosition >= d.Core.QueryIndex{
		d.Core.QueryIndexPosition = 0
	}

	twoWords := d.Core.Queries[d.Core.QueryIndexPosition]
	redisQuery := fmt.Sprintf(`FT.SEARCH,%s,%s`, "idx1", twoWords)

	humanLabel := "RediSearch Simple 2 Word Query - English-language Wikipedia:Database page abstracts (random words)"
	humanDesc := fmt.Sprintf("%s. words %s", humanLabel,twoWords)
	d.fillInQuery(qi, humanLabel, humanDesc, redisQuery)
	d.Core.QueryIndexPosition++

}


// fill Query fills the query struct with data
func (d *EnWikiAbstract) fillInQuery(qi query.Query, humanLabel, humanDesc, redisQuery string) {
	q := qi.(*query.RediSearch)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
	q.RedisQuery = []byte(redisQuery)
}
