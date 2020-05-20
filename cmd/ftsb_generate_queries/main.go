// tsbs_generate_queries generates queries for various use cases. Its output will
// be consumed by the corresponding ftsb_run_queries_ program.
package main

import (
	"bufio"
	"encoding/gob"
	"flag"
	"fmt"
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_queries/databases/redisearch"
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_queries/uses/wiki"
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_queries/utils"
	"log"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"
)

var useCaseMatrix = map[string]map[string]utils.QueryFillerMaker{
	"enwiki-abstract": {
		wiki.LabelSimple1WordQuery:         wiki.NewSimple1WordQuery(),
		wiki.LabelTwoWordIntersectionQuery: wiki.NewTwoWordIntersectionQuery(),
		wiki.LabelSimple2WordUnionQuery:    wiki.NewTwoWordUnionQuery(),
		wiki.LabelSimple2WordBarackObama:   wiki.NewSimple2WordBarackObama(),
		wiki.LabelSimple1WordSpellCheck:    wiki.NewSimple1WordSpellCheck(),
	},
	"enwiki-pages": {
		wiki.Label0AggStar: wiki.NewAgg0_PerfQuery(),
		wiki.Label1AggExact1YearPageContributionsByDay:                     wiki.NewAgg1_Exact1YearPageContributionsByDayQuery(),
		wiki.Label2AggExact1MonthDistinctEditorContributionsByHour:         wiki.NewAgg2_Exact1MonthDistinctEditorContributionsByHourQuery(),
		wiki.Label3AggApproximate1MonthDistinctEditorContributionsByHour:   wiki.NewAgg3_Approximate1MonthDistinctEditorContributionsByHourQuery(),
		wiki.Label4AggApproximate1DayEditorContributionsBy5minutes:         wiki.NewAgg4_Approximate1DayEditorContributionsBy5minutesQuery(),
		wiki.Label5AggApproximate1MonthPeriodTop10EditorByNumContributions: wiki.NewAgg5_Approximate1MonthPeriodTop10EditorByNumContributionsQuery(),
		wiki.Label6AggApproximate1MonthPeriodTop10EditorByNamespace:        wiki.NewAgg6_AproximateAllTimeTop10EditorByNamespaceQuery(),
		wiki.Label7Agg1MonthPeriodTop10EditorByAvgRevisionContent:          wiki.NewAgg7_1MonthPeriodTop10EditorByAvgRevisionContentQuery(),
		wiki.Label8AggApproximateAvgEditorContributionsByYear:              wiki.NewAgg8_ApproximateAvgEditorContributionsByYearQuery(),
	},
}

const defaultWriteSize = 4 << 20 // 4 MB

// Program option vars:
var (
	fatal = log.Fatalf

	generator utils.EnWikiAbstractGenerator
	filler    utils.QueryFiller

	queryCount     int
	fileName       string
	stopWordsInput string
	stopWords      []string

	seed                         int64
	debug                        int
	inputfileName                string
	interleavedGenerationGroupID uint
	interleavedGenerationGroups  uint
)

func getGenerator(format string, usecase string, inputfile string, stopwordsbl []string, seed int64, maxQueries int, debug int) utils.EnWikiAbstractGenerator {
	if format == "redisearch" {
		switch usecase {
		case wiki.LabelEnWikiAbstract:
			return redisearch.NewEnWikiAbstract(inputfile, stopwordsbl, seed, maxQueries, debug)
		case wiki.LabelEnWikiPages:
			return redisearch.NewEnWikiPages(inputfile, stopwordsbl, seed, maxQueries, debug)
		default:
			panic(fmt.Sprintf("no document generator specified for format '%s'", format))
		}
	}

	panic(fmt.Sprintf("no document generator specified for format '%s'", format))
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
	// Change the Usage function to print the use case matrix of choices:
	oldUsage := flag.Usage
	flag.Usage = func() {
		oldUsage()

		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "The use case matrix of choices is:\n")
		for uc, queryTypes := range useCaseMatrix {
			for qt := range queryTypes {
				fmt.Fprintf(os.Stderr, "  use case: %s, query type: %s\n", uc, qt)
			}
		}
	}

	var format string
	var useCase string
	var queryType string

	flag.StringVar(&format, "format", "redisearch", "Format to emit. (Choices are in the use case matrix.)")
	flag.StringVar(&useCase, "use-case", "enwiki-abstract", "Use case to model. (Choices are in the use case matrix.)")
	flag.StringVar(&queryType, "query-type", "", "Query type. (Choices are in the use case matrix.)")
	flag.Int64Var(&seed, "seed", 0, "PRNG seed (default, or 0, uses the current timestamp).")
	flag.IntVar(&queryCount, "queries", 1000, "Number of queries to generate.")
	flag.IntVar(&debug, "debug", 0, "Debug printing (choices: 0, 1, 2) (default 0).")

	flag.UintVar(&interleavedGenerationGroupID, "interleaved-generation-group-id", 0, "Group (0-indexed) to perform round-robin serialization within. Use this to scale up databuild generation to multiple processes.")
	flag.UintVar(&interleavedGenerationGroups, "interleaved-generation-groups", 1, "The number of round-robin serialization groups. Use this to scale up databuild generation to multiple processes.")
	flag.StringVar(&inputfileName, "input-file", "", "File name to read the databuild from")
	flag.StringVar(&fileName, "output-file", "", "File name to write generated queries to")
	flag.StringVar(&stopWordsInput, "stop-words", "a,is,the,an,and,are,as,at,be,but,by,for,if,in,into,it,no,not,of,on,or,such,that,their,then,there,these,they,this,to,was,will,with", "When searching, stop-words are ignored and treated as if they were not sent to the query processor. Therefore, to be 100% correct we need to prevent those words to enter a query. This list of stop-words should match the one used for the index creation.")

	flag.Parse()

	if !(interleavedGenerationGroupID < interleavedGenerationGroups) {
		fatal("incorrect interleaved groups configuration")
	}

	if _, ok := useCaseMatrix[useCase]; !ok {
		fatal("invalid use case specifier: '%s'", useCase)
	}

	if _, ok := useCaseMatrix[useCase][queryType]; !ok {
		fatal("invalid query type specifier: '%s'", queryType)
	}

	// the default seed is the current timestamp:
	if seed == 0 {
		seed = int64(time.Now().Nanosecond())
	}

	fmt.Fprintf(os.Stderr, "using random seed %d\n", seed)
	stopWords = strings.Split(stopWordsInput, ",")
	// sort the stopwords for faster search
	sort.Strings(stopWords)
	// Make the query generator:
	generator = getGenerator(format, useCase, inputfileName, stopWords, seed, queryCount, debug)
	filler = useCaseMatrix[useCase][queryType](generator)
}

func main() {
	rand.Seed(seed)
	// Set up bookkeeping:
	stats := make(map[string]int64)

	// Get output writer
	out := GetBufferedWriter(fileName)
	defer func() {
		err := out.Flush()
		if err != nil {
			fatal(err.Error())
		}
	}()

	// Create request instances, serializing them to stdout and collecting
	// counts for each kind. If applicable, only prints queries that
	// belong to this interleaved group id:
	currentInterleavedGroup := uint(0)

	enc := gob.NewEncoder(out)
	for i := 0; i < queryCount; i++ {
		q := generator.GenerateEmptyQuery()
		q = filler.Fill(q)
		if currentInterleavedGroup == interleavedGenerationGroupID {
			err := enc.Encode(q)
			if err != nil {
				fatal("encoder %v", err)
			}
			stats[string(q.HumanLabelName())]++

			if debug == 1 {
				_, err := fmt.Fprintf(os.Stderr, "%s\n", q.HumanLabelName())
				if err != nil {
					fatal(err.Error())
				}
			} else if debug == 2 {
				_, err := fmt.Fprintf(os.Stderr, "%s\n", q.HumanDescriptionName())
				if err != nil {
					fatal(err.Error())
				}
			} else if debug >= 3 {
				_, err := fmt.Fprintf(os.Stderr, "%s\n", q.String())
				if err != nil {
					fatal(err.Error())
				}
			}
		}
		q.Release()

		currentInterleavedGroup++
		if currentInterleavedGroup == interleavedGenerationGroups {
			currentInterleavedGroup = 0
		}
	}

	// Print stats:
	keys := []string{}
	for k := range stats {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		_, err := fmt.Fprintf(os.Stderr, "%s: %d queries\n", k, stats[k])
		if err != nil {
			fatal(err.Error())
		}
	}
}
