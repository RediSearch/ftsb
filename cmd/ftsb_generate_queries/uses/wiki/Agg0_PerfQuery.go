package wiki

import (
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_queries/utils"
	"github.com/RediSearch/ftsb/query"
)

type Agg0_PerfQuery struct {
	core utils.EnWikiAbstractGenerator
}

func NewAgg0_PerfQuery() utils.QueryFillerMaker {
	return func(core utils.EnWikiAbstractGenerator) utils.QueryFiller {
		return &Agg0_PerfQuery{
			core: core,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *Agg0_PerfQuery) Fill(q query.Query) query.Query {
	fc, ok := d.core.(Agg0_PerfQueryFiller)
	if !ok {
		panicUnimplementedQuery(d.core)
	}
	fc.Agg0_PerfQuery(q)
	return q
}
