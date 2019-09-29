package wiki

import (
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_queries/utils"
	"github.com/RediSearch/ftsb/query"
)

// TwoWordIntersectionQuery contains info for filling in simple 2 word queries
type AggTop10EditorByAvgRevisionContentQuery struct {
	core utils.EnWikiAbstractGenerator
}

// NewTwoWordIntersectionQuery produces a new function that produces a new TwoWordIntersectionQuery
func NewAggTop10EditorByAvgRevisionContentQuery() utils.QueryFillerMaker {
	return func(core utils.EnWikiAbstractGenerator) utils.QueryFiller {
		return &AggTop10EditorByAvgRevisionContentQuery{
			core: core,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *AggTop10EditorByAvgRevisionContentQuery) Fill(q query.Query) query.Query {
	fc, ok := d.core.(AggTop10EditorByAvgRevisionContentFiller)
	if !ok {
		panicUnimplementedQuery(d.core)
	}
	fc.AggTop10EditorByAvgRevisionContent(q)
	return q
}

//wiki.LabelAggTop10EditorByAvgRevisionContent: wiki.NewAggTop10EditorByAvgRevisionContentQuery()
