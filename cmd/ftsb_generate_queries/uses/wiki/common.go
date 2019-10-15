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
	//0 This is a extremely expensive query in terms of IO and should be used only for internal perf analysis/improvements
	Label0AggStar = "agg-0-*"
	//1
	Label1AggExact1YearPageContributionsByDay = "agg-1-editor-1year-exact-page-contributions-by-day"
	//2
	Label2AggExact1MonthDistinctEditorContributionsByHour = "agg-2-*-1month-exact-distinct-editors-by-hour"
	//3
	Label3AggApproximate1MonthDistinctEditorContributionsByHour = "agg-3-*-1month-approximate-distinct-editors-by-hour"
	//4
	Label4AggApproximate1DayEditorContributionsBy5minutes = "agg-4-*-1day-approximate-page-contributions-by-5minutes-by-editor-username"
	//5
	Label5AggApproximate1MonthPeriodTop10EditorByNumContributions = "agg-5-*-1month-approximate-top10-editor-usernames"
	//6
	Label6AggApproximate1MonthPeriodTop10EditorByNamespace = "agg-6-*-1month-approximate-top10-editor-usernames-by-namespace"
	//7
	Label7Agg1MonthPeriodTop10EditorByAvgRevisionContent = "agg-7-*-1month-avg-revision-content-length-by-editor-username"
	//8
	Label8AggApproximateAvgEditorContributionsByYear = "agg-8-editor-approximate-avg-editor-contributions-by-year"
)

// for ease of testing
var fatal = log.Fatalf

// TwoWordIntersectionQueryFiller is a type that can fill in a single query
type TwoWordIntersectionQueryFiller interface {
	TwoWordIntersectionQuery(query.Query)
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

type Agg0_PerfQueryFiller interface {
	Agg0_PerfQuery(query.Query)
}

// 1 One year period, Exact Number of contributions by day, ordered chronologically, for a given editor

type Agg1_Exact1YearPageContributionsByDayQueryFiller interface {
	Agg1_Exact1YearPageContributionsByDay(query.Query)
}

// 2 One month period, Exact Number of distinct editors contributions by hour, ordered chronologically
type Agg2_Exact1MonthDistinctEditorContributionsByHourQueryFiller interface {
	Agg2_Exact1MonthDistinctEditorContributionsByHour(query.Query)
}

// 3 One month period, Approximate Number of distinct editors contributions by hour, ordered chronologically
type Agg3_Approximate1MonthDistinctEditorContributionsByHourQueryFiller interface {
	Agg3_Approximate1MonthDistinctEditorContributionsByHour(query.Query)
}

// 4 One day period, Approximate Number of contributions by 5minutes interval by editor username, ordered first chronologically and second alphabetically by Revision editor username
type Agg4_Approximate1DayEditorContributionsBy5minutesQueryFiller interface {
	Agg4_Approximate1DayEditorContributionsBy5minutes(query.Query)
}

// 5 Approximate All time Top 10 Revision editor usernames
type Agg5_Approximate1MonthPeriodTop10EditorByNumContributionsQueryFiller interface {
	Agg5_Approximate1MonthPeriodTop10EditorByNumContributions(query.Query)
}

// 6 Approximate All time Top 10 Revision editor usernames by number of Revisions broken by namespace (TAG field)
type Agg6_Approximate1MonthPeriodTop10EditorByNamespaceQueryFiller interface {
	Agg6_Approximate1MonthPeriodTop10EditorByNamespace(query.Query)
}

// 7 Top 10 editor username by average revision content
type Agg7_1MonthPeriodTop10EditorByAvgRevisionContentQueryFiller interface {
	Agg7_1MonthPeriodTop10EditorByAvgRevisionContent(query.Query)
}

// 8 Approximate average number of contributions a specific each editor makes
type Agg8_ApproximateAvgEditorContributionsByYearQueryFiller interface {
	Agg8_ApproximateAvgEditorContributionsByYear(query.Query)
}

func panicUnimplementedQuery(dg utils.EnWikiAbstractGenerator) {
	panic(fmt.Sprintf("database (%v) does not implement query", reflect.TypeOf(dg)))
}
