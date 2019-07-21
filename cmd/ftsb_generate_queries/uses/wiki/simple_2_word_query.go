package wiki

import (
	"time"

	"github.com/filipecosta90/ftsb/cmd/ftsb_generate_queries/utils"
	"github.com/filipecosta90/ftsb/query"
)

// Simple2WordQuery contains info for filling in single groupby queries
type Simple2WordQuery struct {
	core    utils.EnWikiAbstractGenerator
	metrics int
	hosts   int
	hours   int
}

// NewSimple2WordQuery produces a new function that produces a new Simple2WordQuery
func NewSimple2WordQuery(metrics, hosts, hours int) utils.QueryFillerMaker {
	return func(core utils.EnWikiAbstractGenerator) utils.QueryFiller {
		return &Simple2WordQuery{
			core:    core,
			metrics: metrics,
			hosts:   hosts,
			hours:   hours,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *Simple2WordQuery) Fill(q query.Query) query.Query {
	fc, ok := d.core.(Simple2WordQueryFiller)
	if !ok {
		panicUnimplementedQuery(d.core)
	}
	fc.Simple2WordQuery(q, d.hosts, d.metrics, time.Duration(int64(d.hours)*int64(time.Hour)))
	return q
}
