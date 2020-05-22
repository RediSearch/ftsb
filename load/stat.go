package load

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
	label    []byte
	latency  uint64 // microseconds latency
	error    bool
	timedOut bool
	rx       uint64 // bytes received
	tx       uint64 // bytes received
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
	return c.label
}

func (c *CmdStat) SetLabel(label []byte) {
	c.label = label
}

func NewCmdStat(label []byte, latency uint64, error bool, timedOut bool, rx uint64, tx uint64) *CmdStat {
	return &CmdStat{label: label, latency: latency, error: error, timedOut: timedOut, rx: rx, tx: tx}
}

func NewStat() *Stat {
	cmds := make([]CmdStat, 0, 0)
	return &Stat{
		0, cmds,
	}
}

func (s *Stat) AddEntry(label []byte, latencyUs uint64, error bool, timedOut bool, rx, tx uint64) *Stat {
	s.totalCmds++
	entry := CmdStat{label, latencyUs, error, timedOut, rx, tx}
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
