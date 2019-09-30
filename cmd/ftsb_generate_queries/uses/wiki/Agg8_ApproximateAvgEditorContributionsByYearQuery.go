package wiki

import (
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_queries/utils"
	"github.com/RediSearch/ftsb/query"
)

// TwoWordIntersectionQuery contains info for filling in simple 2 word queries
type Agg8_ApproximateAvgEditorContributionsByYearQuery struct {
	core utils.EnWikiAbstractGenerator
}

// NewTwoWordIntersectionQuery produces a new function that produces a new TwoWordIntersectionQuery
func NewAgg8_ApproximateAvgEditorContributionsByYearQuery() utils.QueryFillerMaker {
	return func(core utils.EnWikiAbstractGenerator) utils.QueryFiller {
		return &Agg8_ApproximateAvgEditorContributionsByYearQuery{
			core: core,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *Agg8_ApproximateAvgEditorContributionsByYearQuery) Fill(q query.Query) query.Query {
	fc, ok := d.core.(Agg8_ApproximateAvgEditorContributionsByYearQueryFiller)
	if !ok {
		panicUnimplementedQuery(d.core)
	}
	fc.Agg8_ApproximateAvgEditorContributionsByYear(q)
	return q
}
