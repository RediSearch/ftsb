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
// object ready to generate Queries.
func NewEnWikiPages(filename string, stopwordsbl []string, seed int64, maxQueries int, debug int) *EnWikiPages {
	return &EnWikiPages{
		wiki.NewWikiPagesReader(filename, stopwordsbl, seed, maxQueries, debug),
	}
}

// GenerateEmptyQuery returns an empty query.RediSearch
func (d *EnWikiPages) GenerateEmptyQuery() query.Query {
	return query.NewRediSearch()
}

// 6 ) AggTop10EditorByAvgRevisionContent does a aggreation for the following
// Aproximate All time Top 10 Revision editor usernames by number of Revions broken by namespace (TAG field)
func (d *EnWikiPages) AggAproximateAllTimeTop10EditorByNamespace(qi query.Query) {
	redisQuery := fmt.Sprintf(`FT.AGGREGATE,6`)
	humanLabel := "RediSearch - Aggregate query # 6 - Aproximate All time Top 10 Revision editor usernames by number of Revions broken by namespace (TAG field)."
	humanDesc := fmt.Sprintf("%s - Full dataset search", humanLabel)
	d.fillInQuery(qi, humanLabel, humanDesc, redisQuery)
}

// 7 ) AggTop10EditorByAvgRevisionContent does a aggreation for the following
// Top 10 editor username by average revision content
func (d *EnWikiPages) AggTop10EditorByAvgRevisionContent(qi query.Query) {
	redisQuery := fmt.Sprintf(`FT.AGGREGATE,7`)
	humanLabel := "RediSearch - Aggregate query # 7 - Top 10 editor username by average revision content."
	humanDesc := fmt.Sprintf("%s - Full dataset search", humanLabel)
	d.fillInQuery(qi, humanLabel, humanDesc, redisQuery)
}

// 8 ) AggAproximateAvgEditorContributionsByYear does a aggreation for the following
// Approximate average number of contributions a specific each editor makes
func (d *EnWikiPages) AggAproximateAvgEditorContributionsByYear(qi query.Query) {

	if d.Core.PagesEditorsIndexPosition >= d.Core.PagesEditorsQueryIndex {
		d.Core.PagesEditorsIndexPosition = 0
	}
	value := d.Core.PagesEditors[d.Core.PagesEditorsIndexPosition]
	redisQuery := fmt.Sprintf(`FT.AGGREGATE,8,%s`, value)

	humanLabel := "RediSearch - Aggregate query # 8 - Aproximate average number of contributions by year each editor makes."
	humanDesc := fmt.Sprintf("%s Used editor: %s", humanLabel, value)
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
