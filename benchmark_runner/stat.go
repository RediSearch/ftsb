package benchmark_runner

// Stat represents one statistical measurement, typically used to store the
// latency of a command
type Stat struct {
	totalCmds uint64
	cmdStats  []CmdStat
}

func (s *Stat) CmdStats() []CmdStat {
	return s.cmdStats
}

func (s *Stat) SetCmdStats(cmdStats []CmdStat) {
	s.cmdStats = cmdStats
}

type CmdStat struct {
	cmdQueryGroup []byte // READ, WRITE, etc...
	cmdQueryId    []byte // R1, R2, etc...
	startTs       uint64 // start timestamp in seconds since epoch
	latency       uint64 // microseconds latency
	error         bool
	timedOut      bool
	rx            uint64 // bytes received
	tx            uint64 // bytes received
}

func (c *CmdStat) StartTs() uint64 {
	return c.startTs
}

func (c *CmdStat) SetStartTs(startTs uint64) {
	c.startTs = startTs
}

func (c *CmdStat) Tx() uint64 {
	return c.tx
}

func (c *CmdStat) SetTx(tx uint64) {
	c.tx = tx
}

func (c *CmdStat) Rx() uint64 {
	return c.rx
}

func (c *CmdStat) SetRx(rx uint64) {
	c.rx = rx
}

func (c *CmdStat) Latency() uint64 {
	return c.latency
}

func (c *CmdStat) SetLatency(latency uint64) {
	c.latency = latency
}

func (c *CmdStat) Label() []byte {
	return c.cmdQueryGroup
}

func (c *CmdStat) SetLabel(label []byte) {
	c.cmdQueryGroup = label
}

func (c *CmdStat) CmdQueryId() []byte {
	return c.cmdQueryId
}

func NewCmdStat(cmdGroup []byte, cmdQueryId []byte, latency uint64, error bool, timedOut bool, rx uint64, tx uint64) *CmdStat {
	return &CmdStat{cmdQueryGroup: cmdGroup, cmdQueryId: cmdQueryId, latency: latency, error: error, timedOut: timedOut, rx: rx, tx: tx}
}

func NewStat() *Stat {
	cmds := make([]CmdStat, 0, 0)
	return &Stat{
		0, cmds,
	}
}

func (s *Stat) AddEntry(cmdGroup []byte, cmdQueryId []byte, startTs, latencyUs uint64, error bool, timedOut bool, rx, tx uint64) *Stat {
	s.totalCmds++
	entry := CmdStat{cmdGroup, cmdQueryId, startTs, latencyUs, error, timedOut, rx, tx}
	s.cmdStats = append(s.cmdStats, entry)
	return s
}

func (s *Stat) GetCmdsCount() uint64 {
	return s.totalCmds
}

func (s *Stat) Merge(stat Stat) {
	s.totalCmds += stat.totalCmds
	s.cmdStats = append(s.cmdStats, stat.cmdStats...)
}

func (s *Stat) AddCmdStatEntry(stat CmdStat) {
	s.cmdStats = append(s.cmdStats, stat)
}
