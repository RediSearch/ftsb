// ftsb_generate_data generates full text search data from pre-specified use cases.
//
// Supported formats:
// RediSearch

// Supported use cases:
// enwiki-abstract:

package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/RediSearch/redisearch-go/redisearch"
	"io"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/RediSearch/ftsb/cmd/ftsb_generate_data/common"
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_data/serialize"
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_data/wiki"
)

const (
	// Output data format choices (alphabetical order)
	formatRediSearch = "redisearch"

	// Use case choices (make sure to update TestGetConfig if adding a new one)
	useCaseEnWikiAbstract = "enwiki-abstract"
	// Use case choices (make sure to update TestGetConfig if adding a new one)
	useCaseEnWikiPages = "enwiki-pages"

	errTotalGroupsZero  = "incorrect interleaved groups configuration: total groups = 0"
	errInvalidGroupsFmt = "incorrect interleaved groups configuration: id %d >= total groups %d"
	errInvalidFormatFmt = "invalid format specifier: %v (valid choices: %v)"

	defaultWriteSize = 4 << 20 // 4 MB
)

// semi-constants
var (
	formatChoices = []string{
		formatRediSearch,
	}
	useCaseChoices = []string{
		useCaseEnWikiAbstract,
		useCaseEnWikiPages,
	}
	// allows for testing
	fatal = log.Fatalf
)

// parseableFlagVars are flag values that need sanitization or re-parsing after
// being set, e.g., to convert from string to time.Time or re-setting the value
// based on a special '0' value
type parseableFlagVars struct {
}

// Program option vars:
var (
	format                         string
	useCase                        string
	profileFile                    string
	seed                           int64
	debug                          int
	interleavedGenerationGroupID   uint
	interleavedGenerationGroupsNum uint
	maxDataPoints                  uint64
	fileName                       string
	inputfileName                  string
)

// parseTimeFromString parses string-represented time of the format 2006-01-02T15:04:05Z07:00
func parseTimeFromString(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		fatal("can not parse time from string '%s': %v", s, err)
		return time.Time{}
	}
	return t.UTC()
}

// validateGroups checks validity of combination groupID and totalGroups
func validateGroups(groupID, totalGroupsNum uint) (bool, error) {
	if totalGroupsNum == 0 {
		// Need at least one group
		return false, fmt.Errorf(errTotalGroupsZero)
	}
	if groupID >= totalGroupsNum {
		// Need reasonable groupID
		return false, fmt.Errorf(errInvalidGroupsFmt, groupID, totalGroupsNum)
	}
	return true, nil
}

// validateFormat checks whether format is valid (i.e., one of formatChoices)
func validateFormat(format string) bool {
	for _, s := range formatChoices {
		if s == format {
			return true
		}
	}
	return false
}

// validateUseCase checks whether use-case is valid (i.e., one of useCaseChoices)
func validateUseCase(useCase string) bool {
	for _, s := range useCaseChoices {
		if s == useCase {
			return true
		}
	}
	return false
}

// GetBufferedWriter returns the buffered Writer that should be used for generated output
func GetBufferedWriter(fileName string) *bufio.Writer {
	// Prepare output file/STDOUT
	if len(fileName) > 0 {
		// Write output to file
		file, err := os.Create(fileName)
		if err != nil {
			fatal("cannot open file for write %s: %v", fileName, err)
		}
		return bufio.NewWriterSize(file, defaultWriteSize)
	}

	// Write output to STDOUT
	return bufio.NewWriterSize(os.Stdout, defaultWriteSize)
}

// Parse args:
func init() {

	flag.StringVar(&format, "format", "redisearch", fmt.Sprintf("Format to emit. (choices: %s)", strings.Join(formatChoices, ", ")))

	flag.StringVar(&useCase, "use-case", "enwiki-abstract", fmt.Sprintf("Use case to model. (choices: %s)", strings.Join(useCaseChoices, ", ")))

	flag.IntVar(&debug, "debug", 0, "Debug printing (choices: 0, 1, 2). (default 0)")

	flag.UintVar(&interleavedGenerationGroupID, "interleaved-generation-group-id", 0,
		"Group (0-indexed) to perform round-robin serialization within. Use this to scale up data generation to multiple processes.")
	flag.UintVar(&interleavedGenerationGroupsNum, "interleaved-generation-groups", 1,
		"The number of round-robin serialization groups. Use this to scale up data generation to multiple processes.")

	flag.StringVar(&profileFile, "profile-file", "", "File to which to write go profiling data")
	flag.Int64Var(&seed, "seed", 0, "PRNG seed (default, or 0, uses the current timestamp).")

	flag.Uint64Var(&maxDataPoints, "max-documents", 0, "Limit the number of documentsto generate, 0 = no limit")
	flag.StringVar(&inputfileName, "input-file", "", "File name to read the data from")
	flag.StringVar(&fileName, "output-file", "", "File name to write generated data to")

	flag.Parse()

}

func main() {
	if ok, err := validateGroups(interleavedGenerationGroupID, interleavedGenerationGroupsNum); !ok {
		fatal("incorrect interleaved groups specification: %v", err)
	}
	if ok := validateFormat(format); !ok {
		fatal("invalid format specified: %v (valid choices: %v)", format, formatChoices)
	}
	if ok := validateUseCase(useCase); !ok {
		fatal("invalid use-case specified: %v (valid choices: %v)", useCase, useCaseChoices)
	}

	if len(profileFile) > 0 {
		defer startMemoryProfile(profileFile)()
	}

	rand.Seed(seed)

	// Get output writer
	out := GetBufferedWriter(fileName)
	defer func() {
		err := out.Flush()
		if err != nil {
			fatal(err.Error())
		}
	}()

	cfg := getConfig(useCase)
	sim := cfg.NewSimulator(maxDataPoints, inputfileName, debug)
	serializer := getSerializer(sim, format, useCase, out)
	runSimulator(sim, useCase, serializer, out, interleavedGenerationGroupID, interleavedGenerationGroupsNum)
}

func runSimulator(sim common.Simulator, useCase string, serializer serialize.DocumentSerializer, out io.Writer, groupID, totalGroups uint) {
	currGroupID := uint(0)

	doc := redisearch.NewDocument("1", 1 )

	for !sim.Finished() {

		write := sim.Next(&doc)
		if !write {
			continue
		}

		// in the default case this is always true
		if currGroupID == groupID {
			err := serializer.Serialize(&doc, out)
			if err != nil {
				fatal("can not serialize wikiPages: %s", err)
				return
			}
		}

		currGroupID = (currGroupID + 1) % totalGroups
	}

}

func getConfig(useCase string) common.SimulatorConfig {
	switch useCase {
	case useCaseEnWikiAbstract:
		return &wiki.WikiAbstractSimulatorConfig{
			fileName,
		}
	default:
		fatal("unknown use case: '%s'", useCase)
		return nil
	}
}

func getSerializer(sim common.Simulator, format string, useCase string, out *bufio.Writer) serialize.DocumentSerializer {
	switch format {
	case formatRediSearch:
		switch useCase {
		case useCaseEnWikiAbstract:
			return &serialize.RediSearchDocumentSerializer{}
		default:
			fatal("unknown use case: '%s'", useCase)
			return nil
		}
	default:
		fatal("unknown format: '%s'", format)
		return nil
	}
}

// startMemoryProfile sets up memory profiling to be written to profileFile. It
// returns a function to cleanup/write that should be deferred by the caller
func startMemoryProfile(profileFile string) func() {
	f, err := os.Create(profileFile)
	if err != nil {
		log.Fatal("could not create memory profile: ", err)
	}

	stop := func() {
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
		f.Close()
	}

	// Catches ctrl+c signals
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		<-c

		fmt.Fprintln(os.Stderr, "\ncaught interrupt, stopping profile")
		stop()

		os.Exit(0)
	}()

	return stop
}
