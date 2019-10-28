package redisearch

import (
	"fmt"
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_data/wiki"
	"github.com/RediSearch/ftsb/query"
	"math/rand"
)

// EnWikiAbstract produces RediSearch-specific queries for all the en wiki query types.
type EnWikiAbstract struct {
	Core *wiki.Core
}

// NewEnWikiAbstract makes an EnWikiAbstract object ready to generate TwoWordIntersectionQueries.
func NewEnWikiAbstract(filename string, stopwordsbl []string, seed int64, maxQueries int, debug int) *EnWikiAbstract {
	rand.Seed(seed)
	return &EnWikiAbstract{
		wiki.NewWikiAbrastractReader(filename, stopwordsbl, seed, maxQueries, debug),
	}
}

// GenerateEmptyQuery returns an empty query.RediSearch
func (d *EnWikiAbstract) GenerateEmptyQuery() query.Query {
	return query.NewRediSearch()
}

// TwoWordIntersectionQuery does a search with 2 random words that exist on the set of documents
func (d *EnWikiAbstract) TwoWordUnionQuery(qi query.Query) {
	if d.Core.TwoWordQueryIndexPosition >= d.Core.TwoWordQueryIndex {
		d.Core.TwoWordQueryIndexPosition = 0
	}
	twoWords := d.Core.TwoWordQueries[d.Core.TwoWordQueryIndexPosition]
	d.Core.TwoWordQueryIndexPosition++

	redisQuery := fmt.Sprintf(`FT.SEARCH,%s|%s`, twoWords[0], twoWords[1])

	humanLabel := "RediSearch 2 Word Union Query - English-language Wikipedia:Database page abstracts (random in set words)."
	humanDesc := fmt.Sprintf("%s Used words: %s", humanLabel, twoWords)
	d.fillInQuery(qi, humanLabel, humanDesc, redisQuery)
}

// TwoWordIntersectionQuery does a search with 2 random words that exist on the set of documents
func (d *EnWikiAbstract) TwoWordIntersectionQuery(qi query.Query) {
	if d.Core.TwoWordQueryIndexPosition >= d.Core.TwoWordQueryIndex {
		d.Core.TwoWordQueryIndexPosition = 0
	}
	twoWords := d.Core.TwoWordQueries[d.Core.TwoWordQueryIndexPosition]
	d.Core.TwoWordQueryIndexPosition++

	redisQuery := fmt.Sprintf(`FT.SEARCH,%s %s`, twoWords[0], twoWords[1])

	humanLabel := "RediSearch 2 Word Intersection Query - English-language Wikipedia:Database page abstracts (random in set words)."
	humanDesc := fmt.Sprintf("%s Used words: %s", humanLabel, twoWords)
	d.fillInQuery(qi, humanLabel, humanDesc, redisQuery)

}

// Simple1WordQuery does a search with 1 random word that exist on the set of documents
func (d *EnWikiAbstract) Simple1WordQuery(qi query.Query) {
	if d.Core.OneWordQueryIndexPosition >= d.Core.OneWordQueryIndex {
		d.Core.OneWordQueryIndexPosition = 0
	}
	oneWord := d.Core.OneWordQueries[d.Core.OneWordQueryIndexPosition]
	d.Core.OneWordQueryIndexPosition++

	redisQuery := fmt.Sprintf(`FT.SEARCH,%s`, oneWord)

	humanLabel := "RediSearch Simple 1 Word Query - English-language Wikipedia:Database page abstracts (random in set words)."
	humanDesc := fmt.Sprintf("%s Used words: %s", humanLabel, oneWord)
	d.fillInQuery(qi, humanLabel, humanDesc, redisQuery)

}

// Simple1WordSpellCheck does a search with 1 random word that exist on the set of documents
func (d *EnWikiAbstract) Simple1WordSpellCheck(qi query.Query) {
	if d.Core.OneWordSpellCheckQueryIndexPosition >= d.Core.OneWordSpellCheckQueryIndex {
		d.Core.OneWordSpellCheckQueryIndexPosition = 0
	}
	oneWord := d.Core.OneWordSpellCheckQueries[d.Core.OneWordSpellCheckQueryIndexPosition]
	distance := d.Core.OneWordSpellCheckQueriesDistance[d.Core.OneWordSpellCheckQueryIndexPosition]
	d.Core.OneWordSpellCheckQueryIndexPosition++

	redisQuery := fmt.Sprintf(`FT.SPELLCHECK,%s,DISTANCE,%d`, oneWord, distance)

	humanLabel := "RediSearch Simple 1 Spellcheck Query - English-language Wikipedia:Database page abstracts (random in set words)."
	humanDesc := fmt.Sprintf("%s Misspelled term: %s", humanLabel, oneWord)
	d.fillInQuery(qi, humanLabel, humanDesc, redisQuery)
}

// Simple2WordBarackObama does a search with the 2 fixed words barack obama
func (d *EnWikiAbstract) Simple2WordBarackObama(qi query.Query) {
	redisQuery := fmt.Sprintf(`FT.SEARCH,barack obama`)

	humanLabel := "RediSearch 2 Word Intersection Query - Barack Obama."
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
