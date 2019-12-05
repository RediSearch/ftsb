package common

import (
	"github.com/RediSearch/redisearch-go/redisearch"
	"math/rand"
)

const (
	Letters string = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

// RandomStringSliceChoice returns a random string from the provided slice of string slices.
func RandomStringSliceChoice(s []string) string {
	return s[rand.Intn(len(s))]
}

// RandomByteStringSliceChoice returns a random byte string slice from the provided slice of byte string slices.
func RandomByteStringSliceChoice(s [][]byte) []byte {
	return s[rand.Intn(len(s))]
}

// RandomInt64SliceChoice returns a random int64 from an int64 slice.
func RandomInt64SliceChoice(s []int64) int64 {
	return s[rand.Intn(len(s))]
}

type CommonFTSSimulatorConfig struct {
	InputFilename  string
	Scale          uint64
	Cardinality    uint64
	NumberOfFields uint64
}

type CommonFTSSimulator struct {
	MadeDocuments uint64
	MaxDocuments  uint64
	RecordIndex   uint64
	Records       []redisearch.Document
}

// Finished tells whether we have simulated all the necessary documents
func (s *CommonFTSSimulator) Finished() bool {
	return s.MadeDocuments >= s.MaxDocuments
}

// A FTSSimulator generates data similar to telemetry from Telegraf for only CPU metrics.
// It fulfills the Simulator interface.
type FTSSimulator struct {
	*CommonFTSSimulator
}

// Next advances a WikiAbstract to the next state in the generator.
func (d *FTSSimulator) Next(p *redisearch.Document) bool {
	// Switch to the next document
	if d.RecordIndex >= uint64(len(d.Records)) {
		d.RecordIndex = 0
	}
	return d.PopulateDocument(p)
}

func (s *FTSSimulator) PopulateDocument(p *redisearch.Document) bool {
	record := &s.Records[s.RecordIndex]

	p.Id = record.Id
	p.Score = record.Score
	for key, value := range record.Properties {
		p.Properties[key] = value
	}
	ret := s.RecordIndex < uint64(len(s.Records))
	s.RecordIndex = s.RecordIndex + 1
	s.MadeDocuments = s.MadeDocuments + 1
	return ret
}
