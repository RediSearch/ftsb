package redisearch

import (
	"fmt"
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_data/wiki"
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
func NewEnWikiPages(filename string, stopwordsbl []string, seed int64, maxQueries int, debug int ) *EnWikiPages {
	return &EnWikiPages{wiki.NewWikiPagesReader(filename, stopwordsbl, seed, maxQueries, debug )}
}

// GenerateEmptyQuery returns an empty query.RediSearch
func (d *EnWikiPages) GenerateEmptyQuery() query.Query {
	return query.NewRediSearch()
}

// AggAproximateAvgEditorContributionsByYear does a search with the 2 fixed words barack obama
func (d *EnWikiPages) AggAproximateAvgEditorContributionsByYear(qi query.Query) {

	if d.Core.PagesEditorsIndexPosition >= d.Core.PagesEditorsQueryIndex {
		d.Core.PagesEditorsIndexPosition = 0
	}
	twoWords := d.Core.PagesEditors[d.Core.PagesEditorsIndexPosition]
	redisQuery := fmt.Sprintf(`FT.AGGREGATE,1,%s`, twoWords)

	humanLabel := "RediSearch - Aggregate query - Aproximate average number of contributions by year each editor makes."
	humanDesc := fmt.Sprintf("%s Used words: %s", humanLabel, twoWords)
	d.fillInQuery(qi, humanLabel, humanDesc, redisQuery)
	d.Core.PagesEditorsIndexPosition++

}

// fill Query fills the query struct with data
func (d *EnWikiPages) fillInQuery(qi query.Query, humanLabel, humanDesc, redisQuery string) {
	q := qi.(*query.RediSearch)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
	q.RedisQuery = []byte(redisQuery)
}
