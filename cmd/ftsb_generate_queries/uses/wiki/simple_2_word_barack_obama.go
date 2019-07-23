package wiki

import (
	"time"

	"github.com/filipecosta90/ftsb/cmd/ftsb_generate_queries/utils"
	"github.com/filipecosta90/ftsb/query"
)

// Simple2WordQuery contains info for filling in single groupby queries
type Simple2WordBarackObama struct {
	core    utils.EnWikiAbstractGenerator
	metrics int
	hosts   int
	hours   int
}

// NewSimple2WordQuery produces a new function that produces a new Simple2WordQuery
func NewSimple2WordBarackObama(metrics, hosts, hours int) utils.QueryFillerMaker {
	return func(core utils.EnWikiAbstractGenerator) utils.QueryFiller {
		return &Simple2WordBarackObama{
			core:    core,
			metrics: metrics,
			hosts:   hosts,
			hours:   hours,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *Simple2WordBarackObama) Fill(q query.Query) query.Query {
	fc, ok := d.core.(Simple2WordBarackObamaFiller)
	if !ok {
		panicUnimplementedQuery(d.core)
	}
	fc.Simple2WordBarackObama(q, d.hosts, d.metrics, time.Duration(int64(d.hours)*int64(time.Hour)))
	return q
}
