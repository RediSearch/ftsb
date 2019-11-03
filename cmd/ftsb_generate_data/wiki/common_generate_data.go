package wiki

import (
	"github.com/RediSearch/redisearch-go/redisearch"
	"math/rand"
)

const (
	letters string = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

func NewCore(pagesEditors []string, seed int64, inferiorLimit int64, superiorLimit int64) *Core {
	rand.Seed(seed)
	return &Core{
		PagesEditors:                  pagesEditors,
		PagesEditorsIndexPosition:     0,
		PagesEditorsQueryIndex:        uint64(len(pagesEditors)),
		SuperiorTimeLimitPagesRecords: superiorLimit,
		InferiorTimeLimitPagesRecords: inferiorLimit,
		MaxRandomInterval:             superiorLimit - inferiorLimit,
	}
}

func NewCoreFromAbstract(OneWord []string, TwoWord [][]string, OneWordSpellCheck []string, OneWordSpellCheckDistance []int) *Core {
	return &Core{
		OneWordQueries:            OneWord,
		OneWordQueryIndexPosition: 0,
		OneWordQueryIndex:         uint64(len(OneWord)),

		TwoWordQueries:            TwoWord,
		TwoWordQueryIndexPosition: 0,
		TwoWordQueryIndex:         uint64(len(TwoWord)),

		OneWordSpellCheckQueries:            OneWordSpellCheck,
		OneWordSpellCheckQueriesDistance:    OneWordSpellCheckDistance,
		OneWordSpellCheckQueryIndexPosition: 0,
		OneWordSpellCheckQueryIndex:         uint64(len(OneWordSpellCheck)),
	}
}

// Core is the common component of all generators for all systems
type Core struct {
	// Abstracts Use Case
	TwoWordQueries            [][]string
	TwoWordQueryIndexPosition uint64
	TwoWordQueryIndex         uint64

	OneWordQueries            []string
	OneWordQueryIndexPosition uint64
	OneWordQueryIndex         uint64

	OneWordSpellCheckQueries            []string
	OneWordSpellCheckQueriesDistance    []int
	OneWordSpellCheckQueryIndexPosition uint64
	OneWordSpellCheckQueryIndex         uint64

	// Pages Use Case
	PagesEditors                  []string
	PagesEditorsIndexPosition     uint64
	PagesEditorsQueryIndex        uint64
	SuperiorTimeLimitPagesRecords int64
	InferiorTimeLimitPagesRecords int64
	MaxRandomInterval             int64
}

type commonFTSSimulatorConfig struct {
	InputFilename string
	// Start is the beginning time for the Simulator
}

type commonFTSSimulator struct {
	madeDocuments uint64
	maxDocuments  uint64
	recordIndex   uint64
	records       []redisearch.Document
}

// Finished tells whether we have simulated all the necessary documents
func (s *commonFTSSimulator) Finished() bool {
	return s.madeDocuments >= s.maxDocuments
}

// A FTSSimulator generates data similar to telemetry from Telegraf for only CPU metrics.
// It fulfills the Simulator interface.
type FTSSimulator struct {
	*commonFTSSimulator
}

// Next advances a WikiAbstract to the next state in the generator.
func (d *FTSSimulator) Next(p *redisearch.Document) bool {
	// Switch to the next document
	if d.recordIndex >= uint64(len(d.records)) {
		d.recordIndex = 0
	}
	return d.populateDocument(p)
}

func (s *FTSSimulator) populateDocument(p *redisearch.Document) bool {
	record := &s.records[s.recordIndex]

	p.Id = record.Id
	p.Score = record.Score
	for key, value := range record.Properties {
		p.Properties[key] = value
	}
	ret := s.recordIndex < uint64(len(s.records))
	s.recordIndex = s.recordIndex + 1
	s.madeDocuments = s.madeDocuments + 1
	return ret
}
