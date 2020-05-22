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
	Workers              uint    `json:"Workers"`
	RequestedInsertRatio float64 `json:"RequestedInsertRatio"`
	RequestedUpdateRatio float64 `json:"RequestedUpdateRatio"`
	RequestedDeleteRatio float64 `json:"RequestedDeleteRatio"`

	// DB Spefic Configs
	DBSpecificConfigs map[string]interface{} `json:"DBSpecificConfigs"`

	// Totals
	StartTime        int64  `json:"StartTime"`
	EndTime          int64  `json:"EndTime"`
	DurationMillis   int64  `json:"DurationMillis"`
	TotalOps         uint64 `json:"TotalOps"`
	SetupTotalWrites uint64 `json:"SetupTotalWrites"`
	TotalWrites      uint64 `json:"TotalWrites"`
	TotalUpdates     uint64 `json:"TotalUpdates"`
	TotalReads       uint64 `json:"TotalReads"`
	TotalReadsCursor uint64 `json:"TotalReadsCursor"`
	TotalDeletes     uint64 `json:"TotalDeletes"`
	TotalLatency     uint64 `json:"TotalLatency"`
	TotalBytes       uint64 `json:"TotalBytes"`

	// Overall Ratios
	MeasuredWriteRatio  float64 `json:"MeasuredWriteRatio"`
	MeasuredReadRatio   float64 `json:"MeasuredReadRatio"`
	MeasuredUpdateRatio float64 `json:"MeasuredUpdateRatio"`
	MeasuredDeleteRatio float64 `json:"MeasuredDeleteRatio"`

	// Overall Rates
	OverallAvgOpsRate               float64 `json:"OverallAvgOpsRate"`
	OverallAvgSetupWriteRate        float64 `json:"OverallAvgSetupWriteRate"`
	OverallAvgWriteRate             float64 `json:"OverallAvgWriteRate"`
	OverallAvgReadRate              float64 `json:"OverallAvgReadRate"`
	OverallAvgReadCursorRate        float64 `json:"OverallAvgReadCursorRate"`
	OverallAvgUpdateRate            float64 `json:"OverallAvgUpdateRate"`
	OverallAvgDeleteRate            float64 `json:"OverallAvgDeleteRate"`
	OverallAvgLatency               float64 `json:"OverallAvgLatency"`
	OverallAvgTxByteRate            float64 `json:"OverallAvgTxByteRate"`
	OverallAvgRxByteRate            float64 `json:"OverallAvgRxByteRate"`
	OverallAvgByteRateHumanReadable string  `json:"OverallAvgByteRateHumanReadable"`

	// Time-Series
	OverallOpsRateTs        []DataPoint `json:"OverallOpsRateTs"`
	OverallTxByteRateTs     []DataPoint `json:"OverallTxByteRateTs"`
	OverallRxByteRateTs     []DataPoint `json:"OverallRxByteRateTs"`
	OverallAverageLatencyTs []DataPoint `json:"OverallAverageLatencyTs"`
	SetupWriteRateTs        []DataPoint `json:"SetupWriteRateTs"`
	WriteRateTs             []DataPoint `json:"WriteRateTs"`
	ReadRateTs              []DataPoint `json:"ReadRateTs"`
	ReadCursorRateTs        []DataPoint `json:"ReadCursorRateTs"`
	UpdateRateTs            []DataPoint `json:"UpdateRateTs"`
	DeleteRateTs            []DataPoint `json:"DeleteRateTs"`
}
