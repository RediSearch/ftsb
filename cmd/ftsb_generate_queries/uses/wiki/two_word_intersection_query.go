package wiki

import (
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_queries/utils"
	"github.com/RediSearch/ftsb/query"
)

// TwoWordIntersectionQuery contains info for filling in simple 2 word queries
type TwoWordIntersectionQuery struct {
	core utils.EnWikiAbstractGenerator
}

// NewTwoWordIntersectionQuery produces a new function that produces a new TwoWordIntersectionQuery
func NewTwoWordIntersectionQuery() utils.QueryFillerMaker {
	return func(core utils.EnWikiAbstractGenerator) utils.QueryFiller {
		return &TwoWordIntersectionQuery{
			core: core,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *TwoWordIntersectionQuery) Fill(q query.Query) query.Query {
	fc, ok := d.core.(TwoWordIntersectionQueryFiller)
	if !ok {
		panicUnimplementedQuery(d.core)
	}
	fc.TwoWordIntersectionQuery(q)
	return q
}
