package wiki

import (
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_queries/utils"
	"github.com/RediSearch/ftsb/query"
)

type Agg3_Approximate1MonthDistinctEditorContributionsByHourQuery struct {
	core utils.EnWikiAbstractGenerator
}

func NewNewAgg3_Approximate1MonthDistinctEditorContributionsByHourQuery() utils.QueryFillerMaker {
	return func(core utils.EnWikiAbstractGenerator) utils.QueryFiller {
		return &Agg3_Approximate1MonthDistinctEditorContributionsByHourQuery{
			core: core,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *Agg3_Approximate1MonthDistinctEditorContributionsByHourQuery) Fill(q query.Query) query.Query {
	fc, ok := d.core.(Agg3_Approximate1MonthDistinctEditorContributionsByHourQueryFiller)
	if !ok {
		panicUnimplementedQuery(d.core)
	}
	fc.Agg3_Approximate1MonthDistinctEditorContributionsByHour(q)
	return q
}
