package wiki

import (
	"github.com/filipecosta90/ftsb/cmd/ftsb_generate_data/serialize"
)

type commonFTSSimulatorConfig struct {
	InputFilename string
	// Start is the beginning time for the Simulator
}

type commonFTSSimulator struct {
	madePoints uint64
	maxPoints  uint64
	recordIndex uint64
	records     []serialize.Document


}

// Finished tells whether we have simulated all the necessary points
func (s *commonFTSSimulator) Finished() bool {
	return s.madePoints >= s.maxPoints
}
