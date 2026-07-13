package main

import (
	"encoding/base64"
	"encoding/csv"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/RediSearch/ftsb/benchmark_runner"
	radix "github.com/mediocregopher/radix/v3"
	"golang.org/x/time/rate"
)

// binaryArgMarker prefixes a command argument whose value is base64-encoded
// binary data (e.g. a raw little-endian float32 vector blob for a VECTOR
// field). Raw binary can't travel inside the line-oriented CSV input format
// (embedded newlines break the scanner), so generators emit
// `__b64__<base64>` and the argument is decoded back to raw bytes here,
// right before the command is sent to Redis.
//
// Contract: the payload after the marker MUST be RFC 4648 *standard* base64
// (the `+`/`/` alphabet, padded with `=`). URL-safe or unpadded (raw)
// encodings are rejected. Any value whose literal prefix is `__b64__` is
// treated as a marker, so this token is reserved: a genuine field value that
// begins with it cannot be represented and will be decoded (or rejected).
const binaryArgMarker = "__b64__"

// decodeBinaryArgs base64-decodes every `__b64__`-marked argument in place and
// returns `shrink`, the total number of input bytes removed by decoding (base64
// expands the payload ~4/3 and the 7-byte marker is stripped). The caller
// subtracts `shrink` from its raw-row byte count so the reported wire-byte
// throughput reflects the decoded bytes actually sent to Redis, not the larger
// base64 text.
//
// A marked argument that fails to decode — or that decodes to zero bytes —
// means the input file is corrupt: silently passing it through would ingest
// garbage into Redis. The error is *returned* (not fatal) so the caller can
// honor -continue-on-error, consistent with every other error path here.
func decodeBinaryArgs(args []string) (shrink uint64, err error) {
	for i, arg := range args {
		if strings.HasPrefix(arg, binaryArgMarker) {
			decoded, decErr := base64.StdEncoding.DecodeString(arg[len(binaryArgMarker):])
			if decErr != nil {
				// i indexes into args (argsStr[4:]); +4 gives the CSV column.
				return 0, fmt.Errorf("failed to base64-decode binary argument at CSV field %d: %w", i+4, decErr)
			}
			if len(decoded) == 0 {
				return 0, fmt.Errorf("empty base64 binary argument at CSV field %d: %q", i+4, arg)
			}
			shrink += uint64(len(arg) - len(decoded))
			// Go strings carry arbitrary bytes; radix sends them verbatim.
			args[i] = string(decoded)
		}
	}
	return shrink, nil
}

type processor struct {
	rows           chan string
	cmdChan        chan benchmark_runner.Stat
	wg             *sync.WaitGroup
	vanillaClient  *radix.Pool
	vanillaCluster *radix.Cluster
	clusterTopo    radix.ClusterTopo
}

// getDialOpts returns the common dial options for connections
func getDialOpts() []radix.DialOpt {
	opts := make([]radix.DialOpt, 0)
	if password != "" {
		opts = append(opts, radix.DialAuthPass(password))
	}
	opts = append(opts, radix.DialTimeout(timeout))
	return opts
}

// getCustomConnFunc returns a ConnFunc using the common dial options
func getCustomConnFunc() func(network, addr string) (radix.Conn, error) {
	opts := getDialOpts()
	return func(network, addr string) (radix.Conn, error) {
		return radix.Dial(network, addr, opts...)
	}
}

// createPool creates a new radix.Pool with the standard configuration
func createPool() (*radix.Pool, error) {
	customConnFunc := getCustomConnFunc()
	return radix.NewPool("tcp", host, 1, radix.PoolConnFunc(customConnFunc), radix.PoolPipelineWindow(0, 0), radix.PoolPingInterval(1*time.Hour))
}

// reconnectPool closes the existing pool (if any) and creates a new one.
// Returns the new pool or logs a fatal error if reconnection fails.
func (p *processor) reconnectPool() {
	if p.vanillaClient != nil {
		p.vanillaClient.Close()
	}
	var err error
	p.vanillaClient, err = createPool()
	if err != nil {
		if continueOnErr {
			log.Printf("Error reconnecting to Redis: %v", err)
		} else {
			log.Fatalf("Fatal error reconnecting to Redis: %v", err)
		}
	} else {
		log.Println("Successfully reconnected to Redis after error")
	}
}

func (p *processor) Init(workerNumber int, _ bool, totalWorkers int) {
	var err error = nil

	customConnFunc := getCustomConnFunc()

	// this cluster will use the ClientFunc to create a pool to each node in the
	// cluster.
	poolFunc := func(network, addr string) (radix.Client, error) {
		return radix.NewPool(network, addr, int(1), radix.PoolConnFunc(customConnFunc), radix.PoolPipelineWindow(0, 0))
	}

	if clusterMode {

		// We dont want the cluster to sync during the benchmark so we increase the sync time to a large value ( and do the sync CLUSTER SLOTS ) prior
		p.vanillaCluster, err = radix.NewCluster([]string{host}, radix.ClusterPoolFunc(poolFunc), radix.ClusterSyncEvery(1*time.Hour))
		if err != nil {
			log.Fatalf("Error preparing for redisearch ingestion, while creating new cluster connection. error = %v", err)
		}
		err = p.vanillaCluster.Sync()
		if err != nil {
			log.Fatalf("Error retrieving cluster topology. error = %v", err)
		}
		p.clusterTopo = p.vanillaCluster.Topo()
	} else {
		// add randomness on ping interval
		//pingInterval := (20+rand.Intn(10))*1000000000
		// We dont want PING to be issed from 5 to 5 seconds given that we know the connection is alive on the benchmark
		p.vanillaClient, err = createPool()
		if err != nil {
			log.Fatalf("Error preparing for redisearch ingestion, while creating new pool. error = %v", err)
		}
	}
}

func connectionProcessor(p *processor, rateLimiter *rate.Limiter, useRateLimiter bool) {
	pendingSlots := make([][]pendingCmd, 0, 0)
	clusterSlots := make([][2]uint16, 0, 0)
	clusterAddr := make([]string, 0, 0)
	clusterAddrLen := 0
	slotP := 0
	if !clusterMode {
		pendingSlots = append(pendingSlots, make([]pendingCmd, 0, 0))
	} else {
		for _, ClusterNode := range p.clusterTopo {
			for _, slot := range ClusterNode.Slots {
				clusterSlots = append(clusterSlots, slot)
				pendingSlots = append(pendingSlots, make([]pendingCmd, 0, 0))
				clusterAddr = append(clusterAddr, ClusterNode.Addr)
			}
		}
		clusterAddrLen = len(clusterSlots)
		// start at a random slot between 0 and clusterAddrLen.
		// NOSONAR: math/rand is intentional here — this only spreads the
		// benchmark's starting slot for load distribution; it is not security
		// sensitive and needs no crypto-grade randomness.
		slotP = rand.Intn(clusterAddrLen) // NOSONAR
	}

	for row := range p.rows {
		cmdType, cmdQueryId, keyPos, cmd, key, clusterSlot, docFields, bytelen, err := preProcessCmd(row)
		if err != nil {
			// Honor -continue-on-error like every other error path: skip the
			// bad row rather than nuking an entire (EC2-billed) benchmark run.
			if continueOnErr {
				log.Printf("skipping malformed row: %v", err)
				continue
			}
			log.Fatalf("fatal error preprocessing row: %v", err)
		}

		if clusterSlot > -1 {
			for i, sArr := range clusterSlots {
				if clusterSlot >= int(sArr[0]) && clusterSlot < int(sArr[1]) {
					slotP = i
				}
			}
		} else {
			// round robin slot
			slotP++
			if slotP >= clusterAddrLen {
				slotP = 0
			}
		}

		if debug > 2 {
			fmt.Println(keyPos, slotP, key, clusterSlot, cmd, strings.Join(docFields, ","), clusterSlots)
		}
		if useRateLimiter {
			r := rateLimiter.ReserveN(time.Now(), int(1))
			time.Sleep(r.Delay())
		}
		if !clusterMode {
			var hadError bool
			pendingSlots[slotP], hadError = sendFlatCmd(p, p.vanillaClient, cmdType, cmdQueryId, cmd, docFields, bytelen, pendingSlots[slotP])
			if hadError && continueOnErr {
				// Reconnect to get a fresh connection after an error.
				// This prevents hanging on a broken/half-closed connection
				// (e.g., after OOM errors where the server may close the connection).
				p.reconnectPool()
			}
		} else {
			client, _ := p.vanillaCluster.Client(clusterAddr[slotP])
			pendingSlots[slotP], _ = sendFlatCmd(p, client, cmdType, cmdQueryId, cmd, docFields, bytelen, pendingSlots[slotP])
		}
	}

	// Flush the trailing partial window(s). Without this, the last
	// (rows % pipeline) buffered commands in each slot are never sent to Redis
	// or counted -- silent data loss whenever pipeline does not divide the row
	// count. flushPending sends whatever is buffered regardless of pipeline size.
	if !clusterMode {
		if len(pendingSlots[0]) > 0 {
			var hadError bool
			pendingSlots[0], hadError = flushPending(p, p.vanillaClient, pendingSlots[0])
			if hadError && continueOnErr {
				p.reconnectPool()
			}
		}
	} else {
		for i := range pendingSlots {
			if len(pendingSlots[i]) > 0 {
				client, _ := p.vanillaCluster.Client(clusterAddr[i])
				pendingSlots[i], _ = flushPending(p, client, pendingSlots[i])
			}
		}
	}
	p.wg.Done()
}

// getRxLen approximates the reply bytes received for a command by sizing the
// value radix unmarshalled into the receiver: bulk strings (string/[]byte),
// arrays ([]interface{} / []string, summed recursively), and integers (decimal
// digit count). A *interface{} receiver is dereferenced; nil/unknown → 0.
func getRxLen(v interface{}) (res uint64) {
	switch x := v.(type) {
	case *interface{}:
		if x != nil {
			res = getRxLen(*x)
		}
	case string:
		res = uint64(len(x))
	case []byte:
		res = uint64(len(x))
	case []string:
		for _, i := range x {
			res += uint64(len(i))
		}
	case []interface{}:
		for _, e := range x {
			res += getRxLen(e)
		}
	case int64:
		res = uint64(len(strconv.FormatInt(x, 10)))
	}
	return
}

// pendingCmd is a single command buffered for the current pipeline window,
// carrying everything needed to record its own stat at flush time. Buffering
// per command (instead of threading parallel scalar values through the flush)
// is what makes pipelined accounting correct: every command keeps its own send
// time, sent-byte count, reply receiver, and labels, rather than inheriting the
// flushing command's values.
type pendingCmd struct {
	action     radix.CmdAction
	reply      *interface{}
	cmdType    string
	cmdQueryId string
	redisCmd   string
	redisKey   string
	txBytes    uint64
}

func sendFlatCmd(p *processor, client radix.Client, cmdType, cmdQueryId, cmd string, docfields []string, txBytesCount uint64, pending []pendingCmd) ([]pendingCmd, bool) {
	reply := new(interface{})
	key := ""
	if len(docfields) > 0 {
		key = docfields[0]
	}
	pending = append(pending, pendingCmd{
		action:     radix.Cmd(reply, cmd, docfields...),
		reply:      reply,
		cmdType:    cmdType,
		cmdQueryId: cmdQueryId,
		redisCmd:   cmd,
		redisKey:   key,
		txBytes:    txBytesCount,
	})
	return sendIfRequired(p, client, pending)
}

// sendIfRequired flushes the buffered pipeline window once it reaches `pipeline`
// commands; otherwise it buffers and returns. The trailing partial window (fewer
// than `pipeline` commands, e.g. `rows % pipeline` at end of input) is flushed by
// the caller via flushPending -- see connectionProcessor -- so those commands are
// never silently dropped.
func sendIfRequired(p *processor, client radix.Client, pending []pendingCmd) ([]pendingCmd, bool) {
	if len(pending) < pipeline {
		return pending, false
	}
	return flushPending(p, client, pending)
}

// flooredMicros converts a duration to whole microseconds with a 1us floor: a
// real network round-trip is never 0us, so a measured 0 only reflects
// sub-microsecond timer resolution and would otherwise record a physically
// impossible 0us latency.
func flooredMicros(d time.Duration) uint64 {
	us := uint64(d.Microseconds())
	if us == 0 {
		return 1
	}
	return us
}

// logFlushError logs a pipeline-flush failure, honoring -continue-on-error
// (log-and-continue vs. fatal), and returns whether it was an i/o timeout. A
// flush may mix command types, so the first buffered command is used as the
// summary label. Split out of flushPending to keep that function simple.
func logFlushError(pending []pendingCmd, err error) bool {
	rep := pending[0]
	isRead := rep.cmdType == "READ" || rep.cmdType == "READ_CURSOR"
	// Preserve the historical prefixes: "Fatal error with" on the aborting path
	// (-continue-on-error=false), "Received an error with" when continuing.
	prefix := "Received an error with"
	logf := log.Printf
	if !continueOnErr {
		prefix = "Fatal error with"
		logf = log.Fatalf
	}
	if isRead {
		logf("%s %d command(s) in pipeline, error: %v", prefix, len(pending), err)
	} else {
		logf("%s %s command: %s %s (%d command(s) in pipeline), error: %v", prefix, rep.cmdType, rep.redisCmd, rep.redisKey, len(pending), err)
	}
	if !strings.Contains(err.Error(), "i/o timeout") {
		return false
	}
	if isRead {
		log.Printf("Timeout occurred with %d command(s) in pipeline, continuing execution...", len(pending))
	} else {
		log.Printf("Timeout occurred with %s command: %s %s (%d command(s) in pipeline), continuing execution...", rep.cmdType, rep.redisCmd, rep.redisKey, len(pending))
	}
	return true
}

// flushPending sends the buffered commands (as a pipeline when >1), records one
// stat per command -- each with its own sent/received bytes and labels, plus the
// shared batch send->reply latency -- and returns the emptied buffer (reusing the
// backing array to avoid churn on the hot path). Callers must guard against an
// empty buffer.
func flushPending(p *processor, client radix.Client, pending []pendingCmd) ([]pendingCmd, bool) {
	hadError := false

	// Build the action BEFORE timing so the latency window covers only the
	// round-trip, not client-side slice bookkeeping.
	var action radix.Action
	if len(pending) == 1 {
		action = pending[0].action // no need to pipeline a single command
	} else {
		actions := make([]radix.CmdAction, len(pending))
		for i := range pending {
			actions[i] = pending[i].action
		}
		action = radix.Pipeline(actions...)
	}

	sendT := time.Now()
	err := client.Do(action)
	endT := time.Now()
	isTimeout := false
	if err != nil {
		hadError = true
		isTimeout = logFlushError(pending, err)
	}

	// A pipeline is one client round-trip for the whole batch, so attribute the
	// same send->reply latency to every command in it. Measuring from each
	// command's buffer time instead would fold in client-side queueing (the first
	// command would absorb the whole window-fill wait). For pipeline=1 sendT is
	// effectively the command's send time, so latency is unchanged. Floor to 1us:
	// a real network round-trip is never 0us, so a 0 only reflects sub-microsecond
	// timer resolution.
	took := flooredMicros(endT.Sub(sendT))
	for i := range pending {
		pc := &pending[i]
		// AddEntry takes (..., rx, tx): received bytes, then sent bytes. Each
		// command records its OWN counts and labels.
		rxBytesCount := getRxLen(pc.reply)
		stat := benchmark_runner.NewStat().AddEntry([]byte(pc.cmdType), []byte(pc.cmdQueryId), uint64(sendT.Unix()), took, hadError, isTimeout, rxBytesCount, pc.txBytes)
		p.cmdChan <- *stat
	}

	return pending[:0], hadError
}

// ProcessBatch reads eventsBatches which contain rows of databuild for FT.ADD redis command string
func (p *processor) ProcessBatch(b benchmark_runner.Batch, doLoad bool, rateLimiter *rate.Limiter, useRateLimiter bool) (outstat benchmark_runner.Stat) {
	outstat = *benchmark_runner.NewStat()
	events := b.(*eventsBatch)
	rowCnt := uint64(len(events.rows))
	if doLoad {
		buflen := rowCnt + 1

		p.cmdChan = make(chan benchmark_runner.Stat, buflen)
		p.wg = &sync.WaitGroup{}
		p.rows = make(chan string, buflen)
		p.wg.Add(1)
		go connectionProcessor(p, rateLimiter, useRateLimiter)
		for _, row := range events.rows {
			p.rows <- row
		}
		close(p.rows)
		p.wg.Wait()

		close(p.cmdChan)

		for cmdStat := range p.cmdChan {
			outstat.Merge(cmdStat)
		}
	}
	events.rows = events.rows[:0]
	ePool.Put(events)
	return
}

func (p *processor) Close(_ bool) {
	if p.vanillaClient != nil {
		p.vanillaClient.Close()
	}
	if p.vanillaCluster != nil {
		p.vanillaCluster.Close()
	}
}

func preProcessCmd(row string) (cmdType string, cmdQueryId string, keyPos int, cmd string, key string, clusterSlot int, args []string, bytelen uint64, err error) {
	reader := csv.NewReader(strings.NewReader(row))
	argsStr, err := reader.Read()
	if err != nil {
		return
	}

	// we need at least the cmdType and command
	if len(argsStr) >= 3 {
		cmdType = argsStr[0]
		cmdQueryId = argsStr[1]
		initialPos, _ := strconv.Atoi(argsStr[2])

		keyPos = initialPos + 3
		cmd = argsStr[3]
		clusterSlot = -1
		var shrink uint64
		if len(argsStr) > 4 {
			args = argsStr[4:]
			shrink, err = decodeBinaryArgs(args)
			if err != nil {
				return
			}
			// Guard the key index: a malformed row (keyPos derived from the
			// untrusted pos field) must not panic the whole worker goroutine.
			if keyPos < 0 || keyPos >= len(argsStr) {
				err = fmt.Errorf("key position %d out of range for row with %d fields: %s", keyPos, len(argsStr), row)
				return
			}
			key = argsStr[keyPos]
		}
		if initialPos >= 0 {
			clusterSlot = int(radix.ClusterSlot([]byte(key)))
		}
		// bytelen approximates the sent (TX) payload: the row minus the leading
		// cmdType label, minus the base64 shrink (so it reflects decoded bytes,
		// not the larger base64 text). It is an application-payload proxy, not
		// exact RESP wire bytes — it still counts CSV separators / the queryId
		// and pos columns and omits RESP framing (*N\r\n, per-arg $len\r\n). The
		// error is negligible for large payloads (e.g. vector blobs) but can be
		// sizable for many-tiny-arg commands.
		bytelen = uint64(len(row)) - uint64(len(cmdType)) - shrink
	} else {
		err = fmt.Errorf("input string does not have the minimum required size of 2: %s", row)
	}

	return
}
