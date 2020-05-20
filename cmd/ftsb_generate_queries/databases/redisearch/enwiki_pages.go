package redisearch

import (
	"fmt"
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_data/wiki"
	"github.com/RediSearch/ftsb/query"
	"math/rand"
)

// EnWikiPages
// produces RediSearch-specific queries for all the en wiki query types.
type EnWikiPages struct {
	Core *wiki.Core
}

var (
	OneDaySeconds             int64 = 24 * 60 * 60
	OneMonthSecods            int64 = 30 * OneDaySeconds
	OneYearSeconds            int64 = 365 * OneDaySeconds
	AggregateQuery0HumanLabel       = "0 - Perf * Filter Query (get all records)."
	AggregateQuery1HumanLabel       = "1 - One year period, Exact Number of contributions by day, ordered chronologically, for a given editor."
	AggregateQuery2HumanLabel       = "2 - One month period, Exact Number of distinct editors contributions by hour, ordered chronologically."
	AggregateQuery3HumanLabel       = "3 - One month period, Approximate Number of distinct editors contributions by hour, ordered chronologically."
	AggregateQuery4HumanLabel       = "4 - One day period, Approximate Number of contributions by 5minutes interval by editor username, ordered first chronologically and second alphabetically by Revision editor username."
	AggregateQuery5HumanLabel       = "5 - One month period, Approximate All time Top 10 Revision editor usernames."
	AggregateQuery6HumanLabel       = "6 - One month period, Approximate All time Top 10 Revision editor usernames by number of Revisions broken by namespace (TAG field)."
	AggregateQuery7HumanLabel       = "7 - One month period, Top 10 editor username by average revision content."
	AggregateQuery8HumanLabel       = "8 - Approximate average number of contributions by year each editor makes."
)

// NewEnWikiPages
// makes an EnWikiPages
// object ready to generate Queries.
func NewEnWikiPages(filename string, stopwordsbl []string, seed int64, maxQueries int, debug int) *EnWikiPages {
	return &EnWikiPages{
		wiki.NewWikiPagesReader(filename, stopwordsbl, seed, maxQueries, debug),
	}
}

func (d *EnWikiPages) getTimeFrame(minMaxInterval int64) (int64, int64) {
	inferiorLimit := int64(rand.Intn(int(d.Core.MaxRandomInterval-minMaxInterval))) + d.Core.InferiorTimeLimitPagesRecords

	superiorLimit := inferiorLimit + minMaxInterval
	return inferiorLimit, superiorLimit
}

// GenerateEmptyQuery returns an empty query.RediSearch
func (d *EnWikiPages) GenerateEmptyQuery() query.Query {
	return query.NewRediSearch()
}

// 0 ) Agg0_PerfQuery does a aggregation for the following
// *
func (d *EnWikiPages) Agg0_PerfQuery(qi query.Query) {
	redisQuery := fmt.Sprintf(`FT.AGGREGATE,0,"*"`)
	humanLabel := "RediSearch - Aggregate query # " + AggregateQuery0HumanLabel
	humanDesc := fmt.Sprintf("%s", humanLabel)
	d.fillInQuery(qi, humanLabel, humanDesc, redisQuery)
}

// 1 ) AggExact1YearPageContributionsByDay does a aggregation for the following
// One year period, Exact Number of contributions by day, ordered chronologically, for a given editor
func (d *EnWikiPages) Agg1_Exact1YearPageContributionsByDay(qi query.Query) {
	if d.Core.PagesEditorsIndexPosition >= d.Core.PagesEditorsQueryIndex {
		d.Core.PagesEditorsIndexPosition = 0
	}
	value := d.Core.PagesEditors[d.Core.PagesEditorsIndexPosition]
	inferiorLimit, superiorLimit := d.getTimeFrame(OneYearSeconds)

	redisQuery := fmt.Sprintf(`FT.AGGREGATE,1,%s,%d,%d`, value, inferiorLimit, superiorLimit)
	humanLabel := "RediSearch - Aggregate query # " + AggregateQuery1HumanLabel
	humanDesc := fmt.Sprintf("%s Used editor: %s, Unix Timeframe Limits [%d,%d]", humanLabel, value, inferiorLimit, superiorLimit)
	d.fillInQuery(qi, humanLabel, humanDesc, redisQuery)
	d.Core.PagesEditorsIndexPosition++
}

// 2 ) AggExact1MonthDistinctEditorContributionsByHour does a aggregation for the following
// One month period, Exact Number of distinct editors contributions by hour, ordered chronologically
func (d *EnWikiPages) Agg2_Exact1MonthDistinctEditorContributionsByHour(qi query.Query) {
	if d.Core.PagesEditorsIndexPosition >= d.Core.PagesEditorsQueryIndex {
		d.Core.PagesEditorsIndexPosition = 0
	}
	inferiorLimit, superiorLimit := d.getTimeFrame(OneMonthSecods)

	redisQuery := fmt.Sprintf(`FT.AGGREGATE,2,%d,%d`, inferiorLimit, superiorLimit)
	humanLabel := "RediSearch - Aggregate query # " + AggregateQuery2HumanLabel
	humanDesc := fmt.Sprintf("%s - Full dataset search, Used Unix Timeframe Limits [%d,%d]", humanLabel, inferiorLimit, superiorLimit)
	d.fillInQuery(qi, humanLabel, humanDesc, redisQuery)
}

// 3 ) AggApproximate1MonthDistinctEditorContributionsByHour does a aggregation for the following
// One month period, Approximate Number of distinct editors contributions by hour, ordered chronologically
func (d *EnWikiPages) Agg3_Approximate1MonthDistinctEditorContributionsByHour(qi query.Query) {
	if d.Core.PagesEditorsIndexPosition >= d.Core.PagesEditorsQueryIndex {
		d.Core.PagesEditorsIndexPosition = 0
	}
	inferiorLimit, superiorLimit := d.getTimeFrame(OneMonthSecods)

	redisQuery := fmt.Sprintf(`FT.AGGREGATE,3,%d,%d`, inferiorLimit, superiorLimit)
	humanLabel := "RediSearch - Aggregate query # " + AggregateQuery3HumanLabel
	humanDesc := fmt.Sprintf("%s - Full dataset search, Used Unix Timeframe Limits [%d,%d]", humanLabel, inferiorLimit, superiorLimit)
	d.fillInQuery(qi, humanLabel, humanDesc, redisQuery)
}

// 4 ) AggApproximate1DayEditorContributionsBy5minutes does a aggregation for the following
// One day period, Approximate Number of contributions by 5minutes interval by editor username, ordered first chronologically and second alphabetically by Revision editor username
func (d *EnWikiPages) Agg4_Approximate1DayEditorContributionsBy5minutes(qi query.Query) {
	if d.Core.PagesEditorsIndexPosition >= d.Core.PagesEditorsQueryIndex {
		d.Core.PagesEditorsIndexPosition = 0
	}
	inferiorLimit, superiorLimit := d.getTimeFrame(OneDaySeconds)

	redisQuery := fmt.Sprintf(`FT.AGGREGATE,4,%d,%d`, inferiorLimit, superiorLimit)
	humanLabel := "RediSearch - Aggregate query # " + AggregateQuery4HumanLabel
	humanDesc := fmt.Sprintf("%s - Full dataset search, Used Unix Timeframe Limits [%d,%d]", humanLabel, inferiorLimit, superiorLimit)
	d.fillInQuery(qi, humanLabel, humanDesc, redisQuery)
}

// 5 ) AggApproximate1DayEditorContributionsBy5minutes does a aggregation for the following
// One month period, Approximate Top 10 Revision editor usernames
func (d *EnWikiPages) Agg5_Approximate1MonthPeriodTop10EditorByNumContributions(qi query.Query) {
	if d.Core.PagesEditorsIndexPosition >= d.Core.PagesEditorsQueryIndex {
		d.Core.PagesEditorsIndexPosition = 0
	}
	inferiorLimit, superiorLimit := d.getTimeFrame(OneMonthSecods)

	redisQuery := fmt.Sprintf(`FT.AGGREGATE,5,%d,%d`, inferiorLimit, superiorLimit)
	humanLabel := "RediSearch - Aggregate query # " + AggregateQuery5HumanLabel
	humanDesc := fmt.Sprintf("%s - Full dataset search, Used Unix Timeframe Limits [%d,%d]", humanLabel, inferiorLimit, superiorLimit)
	d.fillInQuery(qi, humanLabel, humanDesc, redisQuery)
}

// 6 ) AggTop10EditorByAvgRevisionContent does a aggregation for the following
// One month period, Approximate Top 10 Revision editor usernames by number of Revions broken by namespace (TAG field)
func (d *EnWikiPages) Agg6_Approximate1MonthPeriodTop10EditorByNamespace(qi query.Query) {
	if d.Core.PagesEditorsIndexPosition >= d.Core.PagesEditorsQueryIndex {
		d.Core.PagesEditorsIndexPosition = 0
	}
	inferiorLimit, superiorLimit := d.getTimeFrame(OneMonthSecods)
	redisQuery := fmt.Sprintf(`FT.AGGREGATE,6,%d,%d`, inferiorLimit, superiorLimit)

	humanLabel := "RediSearch - Aggregate query # " + AggregateQuery6HumanLabel
	humanDesc := fmt.Sprintf("%s - Full dataset search, Used Unix Timeframe Limits [%d,%d]", humanLabel, inferiorLimit, superiorLimit)
	d.fillInQuery(qi, humanLabel, humanDesc, redisQuery)
}

// 7 ) AggTop10EditorByAvgRevisionContent does a aggreation for the following
// One month period, Top 10 editor username by average revision content
func (d *EnWikiPages) Agg7_1MonthPeriodTop10EditorByAvgRevisionContent(qi query.Query) {
	if d.Core.PagesEditorsIndexPosition >= d.Core.PagesEditorsQueryIndex {
		d.Core.PagesEditorsIndexPosition = 0
	}
	inferiorLimit, superiorLimit := d.getTimeFrame(OneMonthSecods)
	redisQuery := fmt.Sprintf(`FT.AGGREGATE,7,%d,%d`, inferiorLimit, superiorLimit)
	humanLabel := "RediSearch - Aggregate query # " + AggregateQuery7HumanLabel
	humanDesc := fmt.Sprintf("%s - Full dataset search, Used Unix Timeframe Limits [%d,%d]", humanLabel, inferiorLimit, superiorLimit)

	d.fillInQuery(qi, humanLabel, humanDesc, redisQuery)
}

// 8 ) AggApproximateAvgEditorContributionsByYear does a aggreation for the following
// Approximate average number of contributions by year a specific each editor makes
func (d *EnWikiPages) Agg8_ApproximateAvgEditorContributionsByYear(qi query.Query) {

	if d.Core.PagesEditorsIndexPosition >= d.Core.PagesEditorsQueryIndex {
		d.Core.PagesEditorsIndexPosition = 0
	}
	value := d.Core.PagesEditors[d.Core.PagesEditorsIndexPosition]
	redisQuery := fmt.Sprintf(`FT.AGGREGATE,8,%s`, value)

	humanLabel := "RediSearch - Aggregate query # " + AggregateQuery8HumanLabel
	humanDesc := fmt.Sprintf("%s Used editor: %s", humanLabel, value)
	d.fillInQuery(qi, humanLabel, humanDesc, redisQuery)
	d.Core.PagesEditorsIndexPosition++
}

// fill Query fills the query struct with databuild
func (d *EnWikiPages) fillInQuery(qi query.Query, humanLabel, humanDesc, redisQuery string) {
	q := qi.(*query.RediSearch)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
	q.RedisQuery = []byte(redisQuery)
}
