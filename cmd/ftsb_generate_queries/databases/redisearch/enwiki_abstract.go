package redisearch

import (
	"fmt"
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_data/wiki"
	wiki2 "github.com/RediSearch/ftsb/cmd/ftsb_generate_queries/uses/wiki"
	"github.com/RediSearch/ftsb/query"
	"math"
	"math/rand"
)

// EnWikiAbstract produces RediSearch-specific queries for all the en wiki query types.
type EnWikiAbstract struct {
	Core *wiki.Core
}

// NewEnWikiAbstract makes an EnWikiAbstract object ready to generate TwoWordIntersectionQueries.
func NewEnWikiAbstract(filename string, stopwordsbl []string, seed int64, maxQueries int, debug int) *EnWikiAbstract {
	return &EnWikiAbstract{wiki2.NewWikiAbrastractReader(filename, stopwordsbl, seed, maxQueries, debug)}
}

// GenerateEmptyQuery returns an empty query.RediSearch
func (d *EnWikiAbstract) GenerateEmptyQuery() query.Query {
	return query.NewRediSearch()
}

// TwoWordIntersectionQuery does a search with 2 random words that exist on the set of documents
func (d *EnWikiAbstract) TwoWordUnionQuery(qi query.Query) {
	if d.Core.TwoWordUnionQueryIndexPosition >= d.Core.TwoWordUnionQueryIndex {
		d.Core.TwoWordUnionQueryIndexPosition = 0
	}
	twoWords := d.Core.TwoWordUnionQueries[d.Core.TwoWordUnionQueryIndexPosition]
	redisQuery := fmt.Sprintf(`FT.SEARCH,%s`, twoWords)

	humanLabel := "RediSearch 2 Word Union Query - English-language Wikipedia:Database page abstracts (random in set words)."
	humanDesc := fmt.Sprintf("%s Used words: %s", humanLabel, twoWords)
	d.fillInQuery(qi, humanLabel, humanDesc, redisQuery)
	d.Core.TwoWordUnionQueryIndexPosition++
}

// TwoWordIntersectionQuery does a search with 2 random words that exist on the set of documents
func (d *EnWikiAbstract) TwoWordIntersectionQuery(qi query.Query) {
	if d.Core.TwoWordIntersectionQueryIndexPosition >= d.Core.TwoWordIntersectionQueryIndex {
		d.Core.TwoWordIntersectionQueryIndexPosition = 0
	}
	twoWords := d.Core.TwoWordIntersectionQueries[d.Core.TwoWordIntersectionQueryIndexPosition]
	redisQuery := fmt.Sprintf(`FT.SEARCH,%s`, twoWords)

	humanLabel := "RediSearch 2 Word Intersection Query - English-language Wikipedia:Database page abstracts (random in set words)."
	humanDesc := fmt.Sprintf("%s Used words: %s", humanLabel, twoWords)
	d.fillInQuery(qi, humanLabel, humanDesc, redisQuery)
	d.Core.TwoWordIntersectionQueryIndexPosition++
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

// Simple1WordSpellCheck does a search with 1 random word that exist on the set of documents
func (d *EnWikiAbstract) Simple1WordSpellCheck(qi query.Query) {
	if d.Core.OneWordQueryIndexPosition >= d.Core.OneWordQueryIndex {
		d.Core.OneWordQueryIndexPosition = 0
	}
	oneWord := d.Core.OneWordQueries[d.Core.OneWordQueryIndexPosition]
	//newWord := make(string, len(oneWord))
	var newWord = oneWord
	maxChanges := math.Min(float64(len(oneWord)-2), 4)
	numberChanges := 1
	effectiveChanges := 0
	if maxChanges > 0 {
		numberChanges = rand.Intn(int(maxChanges))
		// the word needs to have at least 4 chars
		for atChange := 0; atChange < numberChanges; atChange++ {
			if len(newWord) > 3 {
				charPos := rand.Intn(len(newWord)-2) + 1
				// non-negative pseudo-random number in [0,4)
				// 0 - delete char word[:charPos] + word[:charPos+1:]
				// 1 - insert random char
				// 2 - replace with random char
				// 3 - switch adjacent chars
				switch rand.Intn(4) {
				case 0:
					newWord = newWord[:charPos] + newWord[charPos+1:]
				case 1:
					newWord = newWord[:charPos] + string(letters[rand.Intn(len(letters))]) + newWord[charPos+1:]
				case 2:
					newWord = newWord[:charPos] + string(letters[rand.Intn(len(letters))]) + newWord[charPos+1:]
				case 3:
					adjacentPos := charPos + 1
					newWord = newWord[:charPos] + newWord[adjacentPos:adjacentPos] + newWord[charPos:charPos] + newWord[adjacentPos+1:]
				}
				effectiveChanges = effectiveChanges + 1
			}
		}

	}

	redisQuery := fmt.Sprintf(`FT.SPELLCHECK,%s,DISTANCE,%d`, newWord, effectiveChanges+1)

	humanLabel := "RediSearch Simple 1 Spellcheck Query - English-language Wikipedia:Database page abstracts (random in set words)."
	humanDesc := fmt.Sprintf("%s Original word: %s, Misspelled term: %s", humanLabel, oneWord, newWord)
	d.fillInQuery(qi, humanLabel, humanDesc, redisQuery)
	d.Core.OneWordQueryIndexPosition++
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
