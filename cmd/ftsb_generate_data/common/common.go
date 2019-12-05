package common

import (
	"code.cloudfoundry.org/bytefmt"
	"fmt"
	"github.com/RediSearch/redisearch-go/redisearch"
	"math/rand"
	"os"
)

const (
	Letters string = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

// RandomStringSliceChoice returns a random string from the provided slice of string slices.
func RandomStringSliceChoice(s []string) string {
	return s[rand.Intn(len(s))]
}

func RandomStringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
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

// Finished tells whether we have simulated all the necessary documents
func (s *CommonFTSSimulator) Describe(file *os.File) {
	fmt.Fprintf(file, "-------------- Dataset description --------------\n")
	fmt.Fprintf(file, "Total Documents: %d\n", len(s.Records))
	nfields := 0
	for i := 0; i < len(s.Records); i++ {
		nfields = nfields + len(s.Records[i].Properties)
	}
	fmt.Fprintf(file, "Avg. Number Fields per Document: %.1f\n", float64(nfields)/float64(len(s.Records)))
	size := 0
	for i := 0; i < len(s.Records); i++ {
		size = size + s.Records[i].EstimateSize()
	}
	fmt.Fprintf(file, "Expected Documents Size: %s\n", bytefmt.ByteSize(uint64(size)))
	fmt.Fprintf(file, "Expected Avg. Documents Size: %s\n", bytefmt.ByteSize(uint64(size/len(s.Records))))

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
