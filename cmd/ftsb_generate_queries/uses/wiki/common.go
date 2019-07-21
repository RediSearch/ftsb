package wiki

import (
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"time"

	"github.com/filipecosta90/ftsb/cmd/ftsb_generate_queries/utils"
	"github.com/filipecosta90/ftsb/query"
)

const (
	allHosts                = "all hosts"
	errNHostsCannotNegative = "nHosts cannot be negative"
	errNoMetrics            = "cannot get 0 metrics"
	errTooManyMetrics       = "too many metrics asked for"
	errBadTimeOrder         = "bad time order: start is after end"
	errMoreItemsThanScale   = "cannot get random permutation with more items than scale"

	// LabelSimple1WordQuery is the label prefix for queries of the Simple 1 Word Query
	LabelSimple1WordQuery = "simple-1word-query"
	// LabelSimple2WordQuery is the label prefix for queries of the Simple 2 Word Query
	LabelSimple2WordQuery = "simple-2word-query"
	// LabelExact3WordMatch is the label for the lastpoint query
	LabelExact3WordMatch = "exact-3word-match"
	// LabelAutocomplete1100Top3 is the label prefix for queries of the max all variety
	LabelAutocomplete1100Top3 = "autocomplete-1100-top3"
	// LabelGroupbyOrderbyLimit is the label for groupby-orderby-limit query
)

// for ease of testing
var fatal = log.Fatalf

// Core is the common component of all generators for all systems
type Core struct {


	// Scale is the cardinality of the dataset in terms of devices/hosts
	Scale int
}

// NewCore returns a new Core for the given time range and cardinality
func NewCore( filename string ) *Core {

	return &Core{
		1,
	}
}

// GetRandomHosts returns a random set of nHosts from a given Core
func (d *Core) GetRandomHosts(nHosts int) []string {
	return getRandomHosts(nHosts, d.Scale)
}

// cpuMetrics is the list of metric names for CPU
var cpuMetrics = []string{
	"usage_user",
	"usage_system",
	"usage_idle",
	"usage_nice",
	"usage_iowait",
	"usage_irq",
	"usage_softirq",
	"usage_steal",
	"usage_guest",
	"usage_guest_nice",
}

// GetCPUMetricsSlice returns a subset of metrics for the CPU
func GetCPUMetricsSlice(numMetrics int) []string {
	if numMetrics <= 0 {
		fatal(errNoMetrics)
		return nil
	}
	if numMetrics > len(cpuMetrics) {
		fatal(errTooManyMetrics)
		return nil
	}
	return cpuMetrics[:numMetrics]
}

// GetAllCPUMetrics returns all the metrics for CPU
func GetAllCPUMetrics() []string {
	return cpuMetrics
}

// GetCPUMetricsLen returns the number of metrics in CPU
func GetCPUMetricsLen() int {
	return len(cpuMetrics)
}

// Simple2WordQueryFiller is a type that can fill in a single groupby query
type Simple2WordQueryFiller interface {
	Simple2WordQuery(query.Query, int, int, time.Duration)
}

// getRandomHosts returns a subset of numHosts hostnames of a permutation of hostnames,
// numbered from 0 to totalHosts.
// Ex.: host_12, host_7, host_25 for numHosts=3 and totalHosts=30 (3 out of 30)
func getRandomHosts(numHosts int, totalHosts int) []string {
	if numHosts < 1 {
		fatal("number of hosts cannot be < 1; got %d", numHosts)
		return nil
	}
	if numHosts > totalHosts {
		fatal("number of hosts (%d) larger than total hosts. See --scale (%d)", numHosts, totalHosts)
		return nil
	}

	randomNumbers := getRandomSubsetPerm(numHosts, totalHosts)

	hostnames := []string{}
	for _, n := range randomNumbers {
		hostnames = append(hostnames, fmt.Sprintf("host_%d", n))
	}

	return hostnames
}

// getRandomSubsetPerm returns a subset of numItems of a permutation of numbers from 0 to totalNumbers,
// e.g., 5 items out of 30. This is an alternative to rand.Perm and then taking a sub-slice,
// which used up a lot more memory and slowed down query generation significantly.
// The subset of the permutation should have no duplicates and thus, can not be longer that original set
// Ex.: 12, 7, 25 for numItems=3 and totalItems=30 (3 out of 30)
func getRandomSubsetPerm(numItems int, totalItems int) []int {
	if numItems > totalItems {
		// Cannot make a subset longer than the original set
		fatal(errMoreItemsThanScale)
		return nil
	}

	seen := map[int]bool{}
	res := []int{}
	for i := 0; i < numItems; i++ {
		for {
			n := rand.Intn(totalItems)
			// Keep iterating until a previously unseen int is found
			if !seen[n] {
				seen[n] = true
				res = append(res, n)
				break
			}
		}
	}
	return res
}

func panicUnimplementedQuery(dg utils.EnWikiAbstractGenerator) {
	panic(fmt.Sprintf("database (%v) does not implement query", reflect.TypeOf(dg)))
}
