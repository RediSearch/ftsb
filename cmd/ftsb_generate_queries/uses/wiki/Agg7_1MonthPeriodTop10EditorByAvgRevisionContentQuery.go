package wiki

import (
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_queries/utils"
	"github.com/RediSearch/ftsb/query"
)

type Agg7_1MonthPeriodTop10EditorByAvgRevisionContentQuery struct {
	core utils.EnWikiAbstractGenerator
}

func NewAgg7_1MonthPeriodTop10EditorByAvgRevisionContentQuery() utils.QueryFillerMaker {
	return func(core utils.EnWikiAbstractGenerator) utils.QueryFiller {
		return &Agg7_1MonthPeriodTop10EditorByAvgRevisionContentQuery{
			core: core,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *Agg7_1MonthPeriodTop10EditorByAvgRevisionContentQuery) Fill(q query.Query) query.Query {
	fc, ok := d.core.(Agg7_1MonthPeriodTop10EditorByAvgRevisionContentQueryFiller)
	if !ok {
		panicUnimplementedQuery(d.core)
	}
	fc.Agg7_1MonthPeriodTop10EditorByAvgRevisionContent(q)
	return q
}
