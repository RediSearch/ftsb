package utils

import "github.com/filipecosta90/ftsb/query"

// EnWikiAbstractGenerator is query generator for a database type that handles the Devops use case
type EnWikiAbstractGenerator interface {
	GenerateEmptyQuery() query.Query
}

// QueryFiller describes a type that can fill in a query and return it
type QueryFiller interface {
	// Fill fills in the query.Query with query details
	Fill(query.Query) query.Query
}

// QueryFillerMaker is a function that takes a EnWikiAbstractGenerator and returns a QueryFiller
type QueryFillerMaker func(EnWikiAbstractGenerator) QueryFiller
