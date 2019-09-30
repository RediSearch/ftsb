package wiki

import (
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_queries/utils"
	"github.com/RediSearch/ftsb/query"
)

// Simple1WordQuery contains info for filling in simple 1 word queries
type Simple1WordSpellCheck struct {
	core utils.EnWikiAbstractGenerator
}

// NewSimple1WordQuery produces a new function that produces a new Simple1WordQuery
func NewSimple1WordSpellCheck() utils.QueryFillerMaker {
	return func(core utils.EnWikiAbstractGenerator) utils.QueryFiller {
		return &Simple1WordSpellCheck{
			core: core,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *Simple1WordSpellCheck) Fill(q query.Query) query.Query {
	fc, ok := d.core.(Simple1WordSpellCheckQueryFiller)
	if !ok {
		panicUnimplementedQuery(d.core)
	}
	fc.Simple1WordSpellCheck(q)
	return q
}
