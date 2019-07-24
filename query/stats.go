package query

import (
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
"github.com/VividCortex/gohistogram"
)

// Stat represents one statistical measurement, typically used to store the
// latency of a query (or part of query).
type Stat struct {
	label     []byte
	value     float64
	totalResults uint64
	isWarm    bool
	isPartial bool
}

var statPool = &sync.Pool{
	New: func() interface{} {
		return &Stat{
			label: make([]byte, 0, 1024),
			value: 0.0,
		}
	},
}

// GetStat returns a Stat for use from a pool
func GetStat() *Stat {
	return statPool.Get().(*Stat).reset()
}

// GetPartialStat returns a partial Stat for use from a pool
func GetPartialStat() *Stat {
	s := GetStat()
	s.isPartial = true
	return s
}

// Init safely initializes a Stat while minimizing heap allocations.
func (s *Stat) Init(label []byte, value float64, totalResults uint64 ) *Stat {
	s.label = s.label[:0] // clear
	s.label = append(s.label, label...)
	s.value = value
	s.totalResults = totalResults
	s.isWarm = false
	return s
}

func (s *Stat) reset() *Stat {
	s.label = s.label[:0]
	s.value = 0.0
	s.totalResults = 0
	s.isWarm = false
	s.isPartial = false
	return s
}

// statGroup collects simple streaming statistics.
type statGroup struct {
	min    float64
	max    float64
	mean   float64
	sum    float64
	sumTotalResults    uint64
	values []float64

	// used for stddev calculations
	m      float64
	s      float64
	stdDev float64

	count int64
	histogram gohistogram.Histogram
	totalResultsHistogram gohistogram.Histogram

}

// newStatGroup returns a new StatGroup with an initial size
func newStatGroup(size uint64) *statGroup {
	return &statGroup{
		values: make([]float64, size),
		count:  0,
		sumTotalResults   : 0,
		histogram:  gohistogram.NewHistogram(5),
		totalResultsHistogram:  gohistogram.NewHistogram(5),
	}
}

// median returns the median value of the StatGroup
func (s *statGroup) median() float64 {
	sort.Float64s(s.values[:s.count])
	if s.count == 0 {
		return 0
	} else if s.count%2 == 0 {
		idx := s.count / 2
		return (s.values[idx] + s.values[idx-1]) / 2.0
	} else {
		return s.values[s.count/2]
	}
}

// push updates a StatGroup with a new value.
func (s *statGroup) push(n float64, totalResults uint64) {
	s.histogram.Add(n)
	s.totalResultsHistogram.Add(float64(totalResults))
	s.sumTotalResults +=totalResults
	if s.count == 0 {
		s.min = n
		s.max = n
		s.mean = n
		s.count = 1
		s.sum = n

		s.m = n
		s.s = 0.0
		s.stdDev = 0.0
		if len(s.values) > 0 {
			s.values[0] = n
		} else {
			s.values = append(s.values, n)
		}
		return
	}

	if n < s.min {
		s.min = n
	}
	if n > s.max {
		s.max = n
	}

	s.sum += n

	// constant-space mean update:
	sum := s.mean*float64(s.count) + n
	s.mean = sum / float64(s.count+1)
	if int(s.count) == len(s.values) {
		s.values = append(s.values, n)
	} else {
		s.values[s.count] = n
	}

	s.count++

	oldM := s.m
	s.m += (n - oldM) / float64(s.count)
	s.s += (n - oldM) * (n - s.m)
	s.stdDev = math.Sqrt(s.s / (float64(s.count) - 1.0))
}

// string makes a simple description of a statGroup.
func (s *statGroup) stringQueryLatency() string {
	return fmt.Sprintf("+ Query execution latency:\n\tmin: %8.2f ms,  mean: %8.2f ms, q25: %8.2f ms, med(q50): %8.2f ms, q75: %8.2f ms, q99: %8.2f ms, max: %8.2f ms, stddev: %8.2fms, sum: %5.3f sec, count: %d\n", s.min, s.mean, s.histogram.Quantile(0.25), s.histogram.Quantile(0.50), s.histogram.Quantile(0.75),  s.histogram.Quantile(0.99), s.max, s.stdDev, s.sum/1e3, s.count)
}

// string makes a simple description of a statGroup.
func (s *statGroup) stringQueryResponseSize() string {
	return fmt.Sprintf("+ Query response size(number docs) statistics:\n\tmin(q0): %8.2f docs, q25: %8.2f docs, med(q50): %8.2f docs, q75: %8.2f docs, q99: %8.2f docs, max(q100): %8.2f docs, sum: %d docs\n", s.totalResultsHistogram.Quantile(0), s.totalResultsHistogram.Quantile(0.25), s.totalResultsHistogram.Quantile(0.50), s.totalResultsHistogram.Quantile(0.75),  s.totalResultsHistogram.Quantile(0.99),  s.totalResultsHistogram.Quantile(1), s.sumTotalResults)
}

func (s *statGroup) write(w io.Writer) error {
	_, err := fmt.Fprintln(w, s.stringQueryLatency())
	_, err = fmt.Fprintln(w, s.stringQueryResponseSize())
	return err
}

// writeStatGroupMap writes a map of StatGroups in an ordered fashion by
// key that they are stored by
func writeStatGroupMap(w io.Writer, statGroups map[string]*statGroup) error {
	maxKeyLength := 0
	keys := make([]string, 0, len(statGroups))
	for k := range statGroups {
		if len(k) > maxKeyLength {
			maxKeyLength = len(k)
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := statGroups[k]
		paddedKey := k
		for len(paddedKey) < maxKeyLength {
			paddedKey += " "
		}

		_, err := fmt.Fprintf(w, "%s:\n", paddedKey)
		if err != nil {
			return err
		}

		err = v.write(w)
		if err != nil {
			return err
		}
	}
	return nil
}
