package wiki

import (
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_queries/utils"
	"github.com/RediSearch/ftsb/query"
)

// TwoWordIntersectionQuery contains info for filling in simple 2 word queries
type Agg6_Approximate1MonthPeriodTop10EditorByNamespaceQuery struct {
	core utils.EnWikiAbstractGenerator
}

// NewTwoWordIntersectionQuery produces a new function that produces a new TwoWordIntersectionQuery
func NewAgg6_AproximateAllTimeTop10EditorByNamespaceQuery() utils.QueryFillerMaker {
	return func(core utils.EnWikiAbstractGenerator) utils.QueryFiller {
		return &Agg6_Approximate1MonthPeriodTop10EditorByNamespaceQuery{
			core: core,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *Agg6_Approximate1MonthPeriodTop10EditorByNamespaceQuery) Fill(q query.Query) query.Query {
	fc, ok := d.core.(Agg6_Approximate1MonthPeriodTop10EditorByNamespaceFiller)
	if !ok {
		panicUnimplementedQuery(d.core)
	}
	fc.Agg6_Approximate1MonthPeriodTop10EditorByNamespace(q)
	return q
}
