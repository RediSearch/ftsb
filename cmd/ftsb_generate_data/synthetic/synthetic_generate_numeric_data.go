package synthetic

import (
	"fmt"
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_data/common"
	"github.com/RediSearch/redisearch-go/redisearch"
	"math/rand"
	"os"
)

// WikiAbstractSimulatorConfig is used to create a FTSSimulator.
type SyntheticNumericSimulatorConfig common.CommonFTSSimulatorConfig

// NewSimulator produces a Simulator that conforms to the given SimulatorConfig over the specified interval
func (c *SyntheticNumericSimulatorConfig) NewSimulator(limit uint64, inputFilename string, debug int, stopwords []string, seed int64) common.Simulator {
	if debug > 0 {
		fmt.Fprintln(os.Stderr, fmt.Sprintf("Using random seed %d", seed))
		fmt.Fprintln(os.Stderr, fmt.Sprintf("stopwords being excluded from generation %s", stopwords))
	}
	var documents []redisearch.Document
	sim := &common.FTSSimulator{&common.CommonFTSSimulator{
		MadeDocuments: 0,
		MaxDocuments:  uint64(len(documents)),
		RecordIndex:   0,
		Records:       documents,
	}}
	if debug > 0 {
		fmt.Fprintln(os.Stderr, fmt.Sprintf("docs generated %d ", uint64(len(documents))))
	}
	return sim
}

// NewSimulator produces a Simulator that conforms to the given SimulatorConfig over the specified interval
func (c *SyntheticNumericSimulatorConfig) NewSyntheticsSimulator(limit uint64, debug int, stopwords []string, numberFields uint64, maxCardinalityPerField uint64, seed int64) common.Simulator {
	if debug > 0 {
		fmt.Fprintln(os.Stderr, fmt.Sprintf("Using random seed %d", seed))
		fmt.Fprintln(os.Stderr, fmt.Sprintf("stopwords being excluded from generation %s", stopwords))
		fmt.Fprintln(os.Stderr, fmt.Sprintf("Preparing to simulate %d docs, with %d fields, and max cardinality per field of %d", limit, numberFields, maxCardinalityPerField))
	}
	rand.Seed(seed)

	var documents []redisearch.Document
	for i := 1; uint64(i) <= limit; i++ {
		var fields []int64
		for j := 1; uint64(j) <= numberFields; j++ {
			var lowerLimit int64 = int64(j) * int64(maxCardinalityPerField)
			value := rand.Int63n(int64(maxCardinalityPerField))
			panValue := lowerLimit + value
			fields = append(fields, panValue)
		}
		documents = append(documents, NewNumericDocument(fmt.Sprintf("doc_%d", i), fields))
	}
	sim := &common.FTSSimulator{&common.CommonFTSSimulator{
		MadeDocuments: 0,
		MaxDocuments:  uint64(len(documents)),
		RecordIndex:   0,
		Records:       documents,
	}}
	if debug > 0 {
		fmt.Fprintln(os.Stderr, fmt.Sprintf("docs generated %d ", uint64(len(documents))))
	}
	return sim
}

func NewNumericDocument(id string, fields []int64) redisearch.Document {
	doc := redisearch.NewDocument(id, 1.0)
	for idx, value := range fields {
		doc = doc.Set(fmt.Sprintf("field_%d", idx+1), value)
	}
	return doc
}
