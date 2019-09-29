package wiki

import (
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_queries/utils"
	"github.com/RediSearch/ftsb/query"
)

type Agg1_Exact1YearPageContributionsByDayQuery struct {
	core utils.EnWikiAbstractGenerator
}

func NewAgg1_Exact1YearPageContributionsByDayQuery() utils.QueryFillerMaker {
	return func(core utils.EnWikiAbstractGenerator) utils.QueryFiller {
		return &Agg1_Exact1YearPageContributionsByDayQuery{
			core: core,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *Agg1_Exact1YearPageContributionsByDayQuery) Fill(q query.Query) query.Query {
	fc, ok := d.core.(Agg1_Exact1YearPageContributionsByDayFiller)
	if !ok {
		panicUnimplementedQuery(d.core)
	}
	fc.Agg1_Exact1YearPageContributionsByDay(q)
	return q
}
