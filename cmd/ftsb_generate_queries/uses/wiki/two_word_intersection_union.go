package wiki

import (
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_queries/utils"
	"github.com/RediSearch/ftsb/query"
)

// TwoWordUnionQuery contains info for filling in a 2 word union
type TwoWordUnionQuery struct {
	core utils.EnWikiAbstractGenerator
}

// NewTwoWordIntersectionQuery produces a new function that produces a new TwoWordUnionQuery
func NewTwoWordUnionQuery() utils.QueryFillerMaker {
	return func(core utils.EnWikiAbstractGenerator) utils.QueryFiller {
		return &TwoWordUnionQuery{
			core: core,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *TwoWordUnionQuery) Fill(q query.Query) query.Query {
	fc, ok := d.core.(TwoWordUnionQueryFiller)
	if !ok {
		panicUnimplementedQuery(d.core)
	}
	fc.TwoWordUnionQuery(q)
	return q
}
