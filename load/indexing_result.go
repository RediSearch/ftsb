package load

type DataPoint struct {
	Timestamp int64   `json:"Timestamp"`
	Value     float64 `json:"Value"`
}

func NewDataPoint(timestamp int64, value float64) *DataPoint {
	return &DataPoint{Timestamp: timestamp, Value: value}
}

type TestResult struct {
	Tag                     string      `json:"Tag"`
	StartTime               int64       `json:"StartTime"`
	Duration                float64     `json:"Duration"`
	BatchSize               int64       `json:"BatchSize"`
	TotalDocuments          uint64      `json:"TotalDocuments"`
	TotalLatency            uint64      `json:"TotalLatency"`
	TotalBytes              uint64      `json:"TotalBytes"`
	AvgIndexingRate         float64     `json:"AvgIndexingRate"`
	OSSDistributedSlots     bool        `json:"OSSDistributedSlots"`
	OverallIngestionRateTs  []DataPoint `json:"OverallIngestionRateTs"`
	OverallByteRateTs       []DataPoint `json:"OverallByteRateTs"`
	OverallAverageLatencyTs []DataPoint `json:"OverallAverageLatencyTs"`
	AddRateTs               []DataPoint `json:"AddRateTs"`
	UpdateRateTs            []DataPoint `json:"UpdateRateTs"`
	DeleteRateTs            []DataPoint `json:"DeleteRateTs"`
}
