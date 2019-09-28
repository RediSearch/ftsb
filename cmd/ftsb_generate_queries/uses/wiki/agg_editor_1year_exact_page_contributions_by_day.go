package wiki

import (
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_queries/utils"
	"github.com/RediSearch/ftsb/query"
)

// TwoWordIntersectionQuery contains info for filling in simple 2 word queries
type AggAproximateAvgEditorContributionsByYearQuery struct {
	core utils.EnWikiAbstractGenerator
}

// NewTwoWordIntersectionQuery produces a new function that produces a new TwoWordIntersectionQuery
func NewAggAproximateAvgEditorContributionsByYearQuery() utils.QueryFillerMaker {
	return func(core utils.EnWikiAbstractGenerator) utils.QueryFiller {
		return &AggAproximateAvgEditorContributionsByYearQuery{
			core: core,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *AggAproximateAvgEditorContributionsByYearQuery) Fill(q query.Query) query.Query {
	fc, ok := d.core.(AggAproximateAvgEditorContributionsByYearFiller)
	if !ok {
		panicUnimplementedQuery(d.core)
	}
	fc.AggAproximateAvgEditorContributionsByYear(q)
	return q
}


//
//NewAggAproximateAllTimeTop10EditorByNamespaceQuery(),
//wiki.LabelAggTop10EditorByAvgRevisionContent: wiki.NewAggTop10EditorByAvgRevisionContentQuery()