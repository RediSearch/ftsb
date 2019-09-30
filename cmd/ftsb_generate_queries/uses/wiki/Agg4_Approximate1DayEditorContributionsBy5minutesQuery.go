package wiki

import (
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_queries/utils"
	"github.com/RediSearch/ftsb/query"
)

type Agg4_Approximate1DayEditorContributionsBy5minutesQuery struct {
	core utils.EnWikiAbstractGenerator
}

func NewAgg4_Approximate1DayEditorContributionsBy5minutesQuery() utils.QueryFillerMaker {
	return func(core utils.EnWikiAbstractGenerator) utils.QueryFiller {
		return &Agg4_Approximate1DayEditorContributionsBy5minutesQuery{
			core: core,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *Agg4_Approximate1DayEditorContributionsBy5minutesQuery) Fill(q query.Query) query.Query {
	fc, ok := d.core.(Agg4_Approximate1DayEditorContributionsBy5minutesQueryFiller)
	if !ok {
		panicUnimplementedQuery(d.core)
	}
	fc.Agg4_Approximate1DayEditorContributionsBy5minutes(q)
	return q
}
