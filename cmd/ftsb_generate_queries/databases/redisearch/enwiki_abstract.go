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

// NewEnWikiAbstract makes an EnWikiAbstract object ready to generate TwoWordQueries.
func NewEnWikiAbstract(filename string, stopwordsbl []string, seed int64, maxQueries int) *EnWikiAbstract {
	return &EnWikiAbstract{wiki.NewCore(filename, stopwordsbl, seed, maxQueries)}
}

// GenerateEmptyQuery returns an empty query.RediSearch
func (d *EnWikiAbstract) GenerateEmptyQuery() query.Query {
	return query.NewRediSearch()
}

// Simple2WordQuery does a search with 2 random words that exist on the set of documents
func (d *EnWikiAbstract) Simple2WordQuery(qi query.Query) {
	if d.Core.TwoWordQueryIndexPosition >= d.Core.TwoWordQueryIndex {
		d.Core.TwoWordQueryIndexPosition = 0
	}
	twoWords := d.Core.TwoWordQueries[d.Core.TwoWordQueryIndexPosition]
	redisQuery := fmt.Sprintf(`FT.SEARCH,%s`, twoWords)

	humanLabel := "RediSearch Simple 2 Word Query - English-language Wikipedia:Database page abstracts (random in set words)."
	humanDesc := fmt.Sprintf("%s Used words: %s", humanLabel, twoWords)
	d.fillInQuery(qi, humanLabel, humanDesc, redisQuery)
	d.Core.TwoWordQueryIndexPosition++
}

// Simple1WordQuery does a search with 1 random word that exist on the set of documents
func (d *EnWikiAbstract) Simple1WordQuery(qi query.Query) {
	if d.Core.OneWordQueryIndexPosition >= d.Core.OneWordQueryIndex {
		d.Core.OneWordQueryIndexPosition = 0
	}
	oneWord := d.Core.OneWordQueries[d.Core.OneWordQueryIndexPosition]
	redisQuery := fmt.Sprintf(`FT.SEARCH,%s`, oneWord)

	humanLabel := "RediSearch Simple 1 Word Query - English-language Wikipedia:Database page abstracts (random in set words)."
	humanDesc := fmt.Sprintf("%s Used words: %s", humanLabel, oneWord)
	d.fillInQuery(qi, humanLabel, humanDesc, redisQuery)
	d.Core.OneWordQueryIndexPosition++
}

// Simple2WordBarackObama does a search with the 2 fixed words barack obama
func (d *EnWikiAbstract) Simple2WordBarackObama(qi query.Query) {
	redisQuery := fmt.Sprintf(`FT.SEARCH,barack obama`)

	humanLabel := "RediSearch Simple 2 Word Query - Barack Obama."
	humanDesc := fmt.Sprintf("%s Used words: barack obama", humanLabel)
	d.fillInQuery(qi, humanLabel, humanDesc, redisQuery)

}

// fill Query fills the query struct with data
func (d *EnWikiAbstract) fillInQuery(qi query.Query, humanLabel, humanDesc, redisQuery string) {
	q := qi.(*query.RediSearch)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
	q.RedisQuery = []byte(redisQuery)
}
