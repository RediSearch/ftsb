package wiki

import (
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_queries/utils"
	"github.com/RediSearch/ftsb/query"
)

// Simple1WordQuery contains info for filling in simple 1 word queries
type Simple1WordQuery struct {
	core utils.EnWikiAbstractGenerator
}

// NewSimple1WordQuery produces a new function that produces a new Simple1WordQuery
func NewSimple1WordQuery() utils.QueryFillerMaker {
	return func(core utils.EnWikiAbstractGenerator) utils.QueryFiller {
		return &Simple1WordQuery{
			core: core,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *Simple1WordQuery) Fill(q query.Query) query.Query {
	fc, ok := d.core.(Simple1WordQueryFiller)
	if !ok {
		panicUnimplementedQuery(d.core)
	}
	fc.Simple1WordQuery(q)
	return q
}
