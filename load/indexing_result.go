package load

type DataPoint struct {
	Timestamp int64   `json:"Timestamp"`
	Value     float64 `json:"Value"`
}

func NewDataPoint(timestamp int64, value float64) *DataPoint {
	return &DataPoint{Timestamp: timestamp, Value: value}
}

type TestResult struct {

	// Test Configs
	Metadata             string  `json:"Metadata"`
	ResultFormatVersion  string  `json:"ResultFormatVersion"`
	BatchSize            int64   `json:"BatchSize"`
	Limit                uint64  `json:"Limit"`
	DbName               string  `json:"DbName"`
	RequestedInsertRatio float64 `json:"RequestedInsertRatio"`
	RequestedUpdateRatio float64 `json:"RequestedUpdateRatio"`
	RequestedDeleteRatio float64 `json:"RequestedDeleteRatio"`

	// DB Spefic Configs
	DBSpecificConfigs map[string]interface{} `json:"DBSpecificConfigs"`

	// Totals
	StartTime      int64  `json:"StartTime"`
	EndTime        int64  `json:"EndTime"`
	DurationMillis int64  `json:"DurationMillis"`
	TotalOps       uint64 `json:"TotalOps"`
	TotalInserts   uint64 `json:"TotalInserts"`
	TotalUpdates   uint64 `json:"TotalUpdates"`
	TotalDeletes   uint64 `json:"TotalDeletes"`
	TotalLatency   uint64 `json:"TotalLatency"`
	TotalBytes     uint64 `json:"TotalBytes"`

	// Overall Ratios
	MeasuredInsertRatio float64 `json:"MeasuredInsertRatio"`
	MeasuredUpdateRatio float64 `json:"MeasuredUpdateRatio"`
	MeasuredDeleteRatio float64 `json:"MeasuredDeleteRatio"`

	// Overall Rates
	OverallAvgOpsRate               float64 `json:"OverallAvgOpsRate"`
	OverallAvgInsertRate            float64 `json:"OverallAvgInsertRate"`
	OverallAvgUpdateRate            float64 `json:"OverallAvgUpdateRate"`
	OverallAvgDeleteRate            float64 `json:"OverallAvgDeleteRate"`
	OverallAvgLatency               float64 `json:"OverallAvgLatency"`
	OverallAvgByteRate              float64 `json:"OverallAvgByteRate"`
	OverallAvgByteRateHumanReadable string  `json:"OverallAvgByteRateHumanReadable"`

	// Time-Series
	OverallIngestionRateTs  []DataPoint `json:"OverallIngestionRateTs"`
	OverallByteRateTs       []DataPoint `json:"OverallByteRateTs"`
	OverallAverageLatencyTs []DataPoint `json:"OverallAverageLatencyTs"`
	InsertRateTs            []DataPoint `json:"InsertRateTs"`
	UpdateRateTs            []DataPoint `json:"UpdateRateTs"`
	DeleteRateTs            []DataPoint `json:"DeleteRateTs"`
}
