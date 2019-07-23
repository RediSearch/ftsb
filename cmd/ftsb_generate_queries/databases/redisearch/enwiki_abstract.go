package redisearch

import (
	"fmt"
	"github.com/filipecosta90/ftsb/cmd/ftsb_generate_queries/uses/wiki"
	"github.com/filipecosta90/ftsb/query"
)

// EnWikiAbstract produces RediSearch-specific queries for all the en wiki query types.
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

// Simple2WordQuery does a search with 2 random words that existe on the set of documents
// every 1 mins for 1 hour
func (d *EnWikiAbstract) Simple2WordQuery(qi query.Query) {
	if d.Core.QueryIndexPosition >= d.Core.QueryIndex{
		d.Core.QueryIndexPosition = 0
	}

	twoWords := d.Core.Queries[d.Core.QueryIndexPosition]
	redisQuery := fmt.Sprintf(`FT.SEARCH,%s`, twoWords)

	humanLabel := "RediSearch Simple 2 Word Query - English-language Wikipedia:Database page abstracts (random in set words)."
	humanDesc := fmt.Sprintf("%s Used words \"%s\"", humanLabel,twoWords)
	d.fillInQuery(qi, humanLabel, humanDesc, redisQuery)
	d.Core.QueryIndexPosition++

}


// Simple2WordBarackObama does a search with the 2 fixed words barack obama
func (d *EnWikiAbstract) Simple2WordBarackObama(qi query.Query) {
	redisQuery := fmt.Sprintf(`FT.SEARCH,barack obama` )

	humanLabel := "RediSearch Simple 2 Word Query - Barack Obama."
	humanDesc := fmt.Sprintf("%s Used words \"barack obama\"", humanLabel)
	d.fillInQuery(qi, humanLabel, humanDesc, redisQuery)

}


// fill Query fills the query struct with data
func (d *EnWikiAbstract) fillInQuery(qi query.Query, humanLabel, humanDesc, redisQuery string) {
	q := qi.(*query.RediSearch)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
	q.RedisQuery = []byte(redisQuery)
}
