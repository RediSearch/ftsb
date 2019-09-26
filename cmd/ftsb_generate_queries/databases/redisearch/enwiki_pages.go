package redisearch

import (
	"fmt"
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_queries/uses/wiki"
	"github.com/RediSearch/ftsb/query"
)

// EnWikiPages
// produces RediSearch-specific queries for all the en wiki query types.
type EnWikiPages struct {
	Core *wiki.Core
}

// NewEnWikiPages
// makes an EnWikiPages
// object ready to generate TwoWordIntersectionQueries.
func NewEnWikiPages(filename string, stopwordsbl []string, seed int64, maxQueries int) *EnWikiPages {
	return &EnWikiPages{wiki.NewWikiAbrastractReader(filename, stopwordsbl, seed, maxQueries)}
}

// GenerateEmptyQuery returns an empty query.RediSearch
func (d *EnWikiPages) GenerateEmptyQuery() query.Query {
	return query.NewRediSearch()
}

// Simple2WordBarackObama does a search with the 2 fixed words barack obama
func (d *EnWikiPages) AggAproximateAvgEditorContributionsByYear(qi query.Query) {
	redisQuery := fmt.Sprintf(`FT.SEARCH,barack obama`)

	humanLabel := "RediSearch - Aggregate query - Aproximate average number of contributions by year each editor makes."
	humanDesc := fmt.Sprintf("%s Used words: barack obama", humanLabel)
	d.fillInQuery(qi, humanLabel, humanDesc, redisQuery)

}

// fill Query fills the query struct with data
func (d *EnWikiPages) fillInQuery(qi query.Query, humanLabel, humanDesc, redisQuery string) {
	q := qi.(*query.RediSearch)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
	q.RedisQuery = []byte(redisQuery)
}
