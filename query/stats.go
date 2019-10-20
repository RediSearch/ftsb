package query

import (
	"fmt"
	"github.com/VividCortex/gohistogram"
	"github.com/filipecosta90/hdrhistogram"
	"io"
	"sort"
	"sync"
	"sync/atomic"
)

// Stat represents one statistical measurement, typically used to store the
// latency of a inference (or part of inference).
type Stat struct {
	label        []byte
	value        int64 // microseconds latency
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
			value:    0,
			timedOut: false,
		}
	},
}

// GetStat returns a Stat for use from a pool
func GetStat() *Stat {
	return statPool.Get().(*Stat).reset()
}

// GetPartialStat returns a partial Stat for use from a pool

// Init safely initializes a Stat while minimizing heap allocations.
func (s *Stat) Init(label []byte, value int64, totalResults uint64, timedOut bool, query string) *Stat {
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
	s.value = 0
	s.totalResults = uint64(0)
	s.isWarm = false
	s.isPartial = false
	return s
}

// statGroup collects simple streaming statistics.
type statGroup struct {
	sumTotalResults                  uint64
	queryDocCountValues              []string
	count                            int64
	timedOutCount                    int64
	totalResultsStatisticalHistogram *gohistogram.NumericHistogram
	latencyHDRHistogram              *hdrhistogram.Histogram
}

// newStatGroup returns a new StatGroup with an initial size
func newStatGroup(size uint64) *statGroup {
	// This latency Histogram could be used to track and analyze the counts of
	// observed integer values between 0 us and 1000000 us ( 1 sec )
	// while maintaining a value precision of 3 significant digits across that range,
	// translating to a value resolution of :
	//   - 1 microsecond up to 1 millisecond,
	//   - 1 millisecond (or better) up to one second,
	lH := hdrhistogram.New(1, 1000000, 3)
	return &statGroup{
		count:                            0,
		timedOutCount:                    0,
		sumTotalResults:                  0,
		latencyHDRHistogram:              lH,
		totalResultsStatisticalHistogram: gohistogram.NewHistogram(1000),
	}
}

// push updates a StatGroup with a new value.
// latency is the latency in microseconds
func (s *statGroup) push(latency_us int64, totalResults uint64, timedOut bool, query string) {
	_ = s.latencyHDRHistogram.RecordValue(latency_us)
	s.totalResultsStatisticalHistogram.Add(float64(totalResults))
	s.sumTotalResults += totalResults
	if timedOut == true {
		s.timedOutCount++
	}
	atomic.AddUint64(&s.sumTotalResults, totalResults)
	atomic.AddInt64(&s.count, 1)
}

// string makes a simple description of a statGroup.
func (s *statGroup) stringQueryLatencyStatistical() string {
	return fmt.Sprintf("+ Query execution latency (statistical histogram):\n\tmin: %8.2f ms,  mean: %8.2f ms, q25: %8.2f ms, med(q50): %8.2f ms, q75: %8.2f ms, q99: %8.2f ms, max: %8.2f ms, stddev: %8.2f ms, count: %d, timedOut count: %d\n",
		float64(s.latencyHDRHistogram.Min())/10e3,
		s.latencyHDRHistogram.Mean()/10e3,
		float64(s.latencyHDRHistogram.ValueAtQuantile(25.0))/10e3,
		float64(s.latencyHDRHistogram.ValueAtQuantile(50.0))/10e3,
		float64(s.latencyHDRHistogram.ValueAtQuantile(75.0))/10e3,
		float64(s.latencyHDRHistogram.ValueAtQuantile(99.0))/10e3,
		float64(s.latencyHDRHistogram.Max())/10e3,
		s.latencyHDRHistogram.StdDev()/10e3,
		s.count, s.timedOutCount)

}

// string makes a simple description of Query Response Size (#docs) of a statGroup.
func (s *statGroup) stringQueryResponseSizeStatistical() string {
	return fmt.Sprintf("+ Query response size(number docs) (statistical histogram):\n\tmin(q0): %8.2f docs, q25: %8.2f docs, med(q50): %8.2f docs, q75: %8.2f docs, q99: %8.2f docs, max(q100): %8.2f docs, sum: %d docs\n",
		s.totalResultsStatisticalHistogram.Quantile(0),
		s.totalResultsStatisticalHistogram.Quantile(0.25),
		s.totalResultsStatisticalHistogram.Quantile(0.50),
		s.totalResultsStatisticalHistogram.Quantile(0.75),
		s.totalResultsStatisticalHistogram.Quantile(0.99),
		s.totalResultsStatisticalHistogram.Quantile(1),
		s.sumTotalResults)
}

// stringQueryResponseSizeFullHistogram returns a string histogram of Query Response Size (#docs)
func (s *statGroup) stringQueryResponseSizeFullHistogram() string {
	return fmt.Sprintf("%s\n", s.totalResultsStatisticalHistogram.String())
}

// stringQueryLatencyFullHistogram returns a string histogram of Query Response Latency in ms
func (s *statGroup) stringQueryLatencyFullHistogram() string {
	return s.latencyHDRHistogram.PercentilesPrint(10, 1000.0)
}

var FormatString1 = "%s,%d\n"

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
