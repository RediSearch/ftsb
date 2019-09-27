package wiki

import (
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_data/serialize"
)

type commonFTSSimulatorConfig struct {
	InputFilename string
	// Start is the beginning time for the Simulator
}

type commonFTSSimulator struct {
	madeDocuments uint64
	maxDocuments  uint64
	recordIndex   uint64
	records       []serialize.WikiAbstract
}

// Finished tells whether we have simulated all the necessary documents
func (s *commonFTSSimulator) Finished() bool {
	return s.madeDocuments >= s.maxDocuments
}
