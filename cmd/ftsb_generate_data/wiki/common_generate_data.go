package wiki

import (
	"github.com/RediSearch/redisearch-go/redisearch"
)

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
