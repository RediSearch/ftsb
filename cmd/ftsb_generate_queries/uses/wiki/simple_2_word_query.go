package wiki

import (
	"github.com/filipecosta90/ftsb/cmd/ftsb_generate_queries/utils"
	"github.com/filipecosta90/ftsb/query"
)

// Simple2WordQuery contains info for filling in simple 2 word queries
type Simple2WordQuery struct {
	core utils.EnWikiAbstractGenerator
}

// NewSimple2WordQuery produces a new function that produces a new Simple2WordQuery
func NewSimple2WordQuery() utils.QueryFillerMaker {
	return func(core utils.EnWikiAbstractGenerator) utils.QueryFiller {
		return &Simple2WordQuery{
			core: core,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *Simple2WordQuery) Fill(q query.Query) query.Query {
	fc, ok := d.core.(Simple2WordQueryFiller)
	if !ok {
		panicUnimplementedQuery(d.core)
	}
	fc.Simple2WordQuery(q)
	return q
}
