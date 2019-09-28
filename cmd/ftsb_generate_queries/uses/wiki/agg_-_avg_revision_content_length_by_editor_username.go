package wiki

import (
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_queries/utils"
	"github.com/RediSearch/ftsb/query"
)

// TwoWordIntersectionQuery contains info for filling in simple 2 word queries
type AggAproximateAllTimeTop10EditorByNamespaceQuery struct {
	core utils.EnWikiAbstractGenerator
}

// NewTwoWordIntersectionQuery produces a new function that produces a new TwoWordIntersectionQuery
func NewAggAproximateAllTimeTop10EditorByNamespaceQuery() utils.QueryFillerMaker {
	return func(core utils.EnWikiAbstractGenerator) utils.QueryFiller {
		return &AggAproximateAllTimeTop10EditorByNamespaceQuery{
			core: core,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *AggAproximateAllTimeTop10EditorByNamespaceQuery) Fill(q query.Query) query.Query {
	fc, ok := d.core.(AggAproximateAllTimeTop10EditorByNamespaceFiller)
	if !ok {
		panicUnimplementedQuery(d.core)
	}
	fc.AggAproximateAllTimeTop10EditorByNamespace(q)
	return q
}

//wiki.LabelAggTop10EditorByAvgRevisionContent: wiki.NewAggTop10EditorByAvgRevisionContentQuery()