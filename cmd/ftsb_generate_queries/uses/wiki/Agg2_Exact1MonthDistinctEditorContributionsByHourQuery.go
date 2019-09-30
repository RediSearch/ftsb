package wiki

import (
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_queries/utils"
	"github.com/RediSearch/ftsb/query"
)

type Agg2_Exact1MonthDistinctEditorContributionsByHourQuery struct {
	core utils.EnWikiAbstractGenerator
}

func NewAgg2_Exact1MonthDistinctEditorContributionsByHourQuery() utils.QueryFillerMaker {
	return func(core utils.EnWikiAbstractGenerator) utils.QueryFiller {
		return &Agg2_Exact1MonthDistinctEditorContributionsByHourQuery{
			core: core,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *Agg2_Exact1MonthDistinctEditorContributionsByHourQuery) Fill(q query.Query) query.Query {
	fc, ok := d.core.(Agg2_Exact1MonthDistinctEditorContributionsByHourQueryFiller)
	if !ok {
		panicUnimplementedQuery(d.core)
	}
	fc.Agg2_Exact1MonthDistinctEditorContributionsByHour(q)
	return q
}
