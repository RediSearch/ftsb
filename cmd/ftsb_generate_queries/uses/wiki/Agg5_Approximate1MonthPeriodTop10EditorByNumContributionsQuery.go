package wiki

import (
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_queries/utils"
	"github.com/RediSearch/ftsb/query"
)

type Agg5_Approximate1MonthPeriodTop10EditorByNumContributionsQuery struct {
	core utils.EnWikiAbstractGenerator
}

func NewAgg5_Approximate1MonthPeriodTop10EditorByNumContributionsQuery() utils.QueryFillerMaker {
	return func(core utils.EnWikiAbstractGenerator) utils.QueryFiller {
		return &Agg5_Approximate1MonthPeriodTop10EditorByNumContributionsQuery{
			core: core,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *Agg5_Approximate1MonthPeriodTop10EditorByNumContributionsQuery) Fill(q query.Query) query.Query {
	fc, ok := d.core.(Agg5_Approximate1MonthPeriodTop10EditorByNumContributionsQueryFiller)
	if !ok {
		panicUnimplementedQuery(d.core)
	}
	fc.Agg5_Approximate1MonthPeriodTop10EditorByNumContributions(q)
	return q
}
