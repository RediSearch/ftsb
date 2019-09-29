package wiki

import (
	"fmt"
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_queries/utils"
	"github.com/RediSearch/ftsb/query"
	"log"
	"reflect"
)

const (
	LabelEnWikiAbstract = "enwiki-abstract"
	LabelEnWikiPages    = "enwiki-pages"
	//////////////////////////
	// Full text search queries
	//////////////////////////
	// LabelSimple1WordQuery is the label prefix for queries of the Simple 1 Word Query
	LabelSimple1WordQuery = "simple-1word-query"
	// LabelTwoWordIntersectionQuery is the label prefix for queries of the Simple 2 Word Intersection Query
	LabelTwoWordIntersectionQuery = "2word-intersection-query"
	// LabelTwoWordIntersectionQuery is the label prefix for queries of the Simple 2 Word Union Query
	LabelSimple2WordUnionQuery = "2word-union-query"
	// LabelExact3WordMatch is the label for the lastpoint query
	LabelExact3WordMatch = "exact-3word-match"
	// LabelAutocomplete1100Top3 is the label prefix for queries of the max all variety
	LabelAutocomplete1100Top3 = "autocomplete-1100-top3"
	// LabelSimple1WordQuery is the label prefix for queries of the Simple 1 Word Query
	LabelSimple2WordBarackObama = "simple-2word-barack-obama"

	//////////////////////////
	// Spell Check queries
	//////////////////////////

	// LabelSimple1WordQuery is the label prefix for queries of the Simple 1 Word Spell Check
	LabelSimple1WordSpellCheck = "simple-1word-spellcheck"

	//////////////////////////
	// Autocomplete queries
	//////////////////////////

	//////////////////////////
	// Synonym queries
	//////////////////////////

	//////////////////////////
	// Aggregation queries
	//////////////////////////

	//6
	LabeAggAproximateAllTimeTop10EditorByNamespace = "agg-*-aproximate-top10-editor-usernames-by-namespace"
	//7
	LabelAggTop10EditorByAvgRevisionContent = "agg-*-avg-revision-content-length-by-editor-username"
	//8
	LabelAggAproximateAvgEditorContributionsByYear = "agg-editor-1year-exact-page-contributions-by-day"
)

// for ease of testing
var fatal = log.Fatalf

// TwoWordIntersectionQueryFiller is a type that can fill in a single query
type TwoWordIntersectionQueryFiller interface {
	TwoWordIntersectionQuery(query.Query)
}

type AggAproximateAvgEditorContributionsByYearFiller interface {
	AggAproximateAvgEditorContributionsByYear(query.Query)
}

type AggAproximateAllTimeTop10EditorByNamespaceFiller interface {
	AggAproximateAllTimeTop10EditorByNamespace(query.Query)
}

type AggTop10EditorByAvgRevisionContentFiller interface {
	AggTop10EditorByAvgRevisionContent(query.Query)
}

// TwoWordUnionQueryFiller is a type that can fill in a single query
type TwoWordUnionQueryFiller interface {
	TwoWordUnionQuery(query.Query)
}

// OneWordQueryFiller is a type that can fill in a single query
type Simple1WordQueryFiller interface {
	Simple1WordQuery(query.Query)
}

// SimpleTwoWordBarackObamaQueryFiller is a type that can fill in a single  query
type Simple2WordBarackObamaFiller interface {
	Simple2WordBarackObama(query.Query)
}

// OneWordQueryFiller is a type that can fill in a single query
type Simple1WordSpellCheckQueryFiller interface {
	Simple1WordSpellCheck(query.Query)
}

func panicUnimplementedQuery(dg utils.EnWikiAbstractGenerator) {
	panic(fmt.Sprintf("database (%v) does not implement query", reflect.TypeOf(dg)))
}
