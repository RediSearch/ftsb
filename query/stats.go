package query

import (
	"fmt"
	"github.com/VividCortex/gohistogram"
	"github.com/grd/histogram"
	"io"
	"math"
	"sort"
	"sync"
	"sync/atomic"
)

// Stat represents one statistical measurement, typically used to store the
// latency of a query (or part of query).
type Stat struct {
	label        []byte
	value        float64
	totalResults uint64
	isWarm       bool
	isPartial    bool
	timedOut     bool
	query        string
}

var statPool = &sync.Pool{
	New: func() interface{} {
		return &Stat{
			label:    make([]byte, 0, 1024),
			value:    0.0,
			timedOut: false,
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
func (s *Stat) Init(label []byte, value float64, totalResults uint64, timedOut bool, query string) *Stat {
	s.query = query
	s.label = s.label[:0] // clear
	s.label = append(s.label, label...)
	s.value = value
	s.totalResults = totalResults
	s.isWarm = false
	s.timedOut = timedOut
	return s
}

func (s *Stat) reset() *Stat {
	s.label = s.label[:0]
	s.value = 0.0
	s.totalResults = uint64(0)
	s.isWarm = false
	s.isPartial = false
	return s
}

// statGroup collects simple streaming statistics.
type statGroup struct {
	min                 float64
	max                 float64
	mean                float64
	sum                 float64
	sumTotalResults     uint64
	values              []float64
	docCountValues      []uint64
	queryDocCountValues []string

	// used for stddev calculations
	m      float64
	s      float64
	stdDev float64

	count                            int64
	timedOutCount                    int64
	latencyStatisticalHistogram      *gohistogram.NumericHistogram
	totalResultsStatisticalHistogram *gohistogram.NumericHistogram
	latencyHistogram                 *histogram.Histogram
	totalResultsHistogram            *histogram.Histogram
	//latencyByResultCount    *histogram.Histogram2d
}

// newStatGroup returns a new StatGroup with an initial size
func newStatGroup(size uint64) *statGroup {
	lH, _ := histogram.NewHistogram(histogram.NaturalRange(0, 3000, 1))
	rH, _ := histogram.NewHistogram(histogram.NaturalRange(0, 1000, 100))
	return &statGroup{
		values:                           make([]float64, size),
		count:                            0,
		timedOutCount:                    0,
		sumTotalResults:                  0,
		latencyStatisticalHistogram:      gohistogram.NewHistogram(1000),
		latencyHistogram:                 lH,
		totalResultsStatisticalHistogram: gohistogram.NewHistogram(1000),
		totalResultsHistogram:            rH,
		docCountValues:                   make([]uint64, 0),
		queryDocCountValues:              make([]string, 0),
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
func (s *statGroup) push(n float64, totalResults uint64, timedOut bool, query string) {
	_ = s.latencyHistogram.Add(n)
	s.latencyStatisticalHistogram.Add(n)
	_ = s.totalResultsHistogram.Add(float64(totalResults))
	s.totalResultsStatisticalHistogram.Add(float64(totalResults))

	s.docCountValues = append(s.docCountValues, totalResults)
	s.queryDocCountValues = append(s.queryDocCountValues, query)
	atomic.AddUint64(&s.sumTotalResults, totalResults)
	if timedOut == true {
		s.timedOutCount++
	}
	if s.count == 0 {
		s.min = n
		s.max = n
		s.mean = n
		atomic.AddInt64(&s.count, 1)

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
	atomic.AddInt64(&s.count, 1)

	oldM := s.m
	s.m += (n - oldM) / float64(s.count)
	s.s += (n - oldM) * (n - s.m)
	s.stdDev = math.Sqrt(s.s / (float64(s.count) - 1.0))
}

// string makes a simple description of a statGroup.
func (s *statGroup) stringQueryLatencyStatistical() string {
	return fmt.Sprintf("+ Query execution latency (statistical histogram):\n\tmin: %8.2f ms,  mean: %8.2f ms, q25: %8.2f ms, med(q50): %8.2f ms, q75: %8.2f ms, q99: %8.2f ms, max: %8.2f ms, stddev: %8.2fms, sum: %5.3f sec, count: %d, timedOut count: %d\n", s.min, s.mean, s.latencyStatisticalHistogram.Quantile(0.25), s.latencyStatisticalHistogram.Quantile(0.50), s.latencyStatisticalHistogram.Quantile(0.75), s.latencyStatisticalHistogram.Quantile(0.99), s.max, s.stdDev, s.sum/1e3, s.count, s.timedOutCount)
}

// string makes a simple description of Query Response Size (#docs) of a statGroup.
func (s *statGroup) stringQueryResponseSizeStatistical() string {
	return fmt.Sprintf("+ Query response size(number docs) (statistical histogram):\n\tmin(q0): %8.2f docs, q25: %8.2f docs, med(q50): %8.2f docs, q75: %8.2f docs, q99: %8.2f docs, max(q100): %8.2f docs, sum: %d docs\n", s.totalResultsStatisticalHistogram.Quantile(0), s.totalResultsStatisticalHistogram.Quantile(0.25), s.totalResultsStatisticalHistogram.Quantile(0.50), s.totalResultsStatisticalHistogram.Quantile(0.75), s.totalResultsStatisticalHistogram.Quantile(0.99), s.totalResultsStatisticalHistogram.Quantile(1), s.sumTotalResults)
}

// stringQueryResponseSizeFullHistogram returns a string histogram of Query Response Size (#docs)
func (s *statGroup) stringQueryResponseSizeFullHistogram() string {
	return fmt.Sprintf("%s\n", s.totalResultsHistogram.String())
}

// stringQueryResponseSizeFullHistogram returns a string histogram of Query Response Size (#docs)
func (s *statGroup) stringQueryLatencyFullHistogram() string {
	return fmt.Sprintf("%s\n", s.latencyHistogram.String())
}

var FormatString1 = "%s,%d\n"

// String uses the variabele FormatString for the data parsing
func (s *statGroup) StringDocCountDebug() (res string) {
	for i := 0; i < len(s.queryDocCountValues); i++ {
		str := fmt.Sprintf(FormatString1, s.queryDocCountValues[i], s.docCountValues[i])
		res += str
	}
	return
}

func (s *statGroup) write(w io.Writer) error {
	_, err := fmt.Fprintln(w, s.stringQueryLatencyStatistical())
	_, err = fmt.Fprintln(w, s.stringQueryResponseSizeStatistical())
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
