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
	cmdSlots := make([][]radix.CmdAction, 0, 0)
	timesSlots := make([][]time.Time, 0, 0)
	replies := make([]interface{}, 0, 0)
	clusterSlots := make([][2]uint16, 0, 0)
	clusterAddr := make([]string, 0, 0)
	clusterAddrLen := 0
	slotP := 0
	if !clusterMode {
		cmdSlots = append(cmdSlots, make([]radix.CmdAction, 0, 0))
		timesSlots = append(timesSlots, make([]time.Time, 0, 0))
	} else {
		for _, ClusterNode := range p.clusterTopo {
			for _, slot := range ClusterNode.Slots {
				clusterSlots = append(clusterSlots, slot)
				cmdSlots = append(cmdSlots, make([]radix.CmdAction, 0, 0))
				timesSlots = append(timesSlots, make([]time.Time, 0, 0))
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
			cmdSlots[slotP], timesSlots[slotP], hadError = sendFlatCmd(p, p.vanillaClient, cmdType, cmdQueryId, cmd, docFields, bytelen, cmdSlots[slotP], replies, timesSlots[slotP])
			if hadError && continueOnErr {
				// Reconnect to get a fresh connection after an error.
				// This prevents hanging on a broken/half-closed connection
				// (e.g., after OOM errors where the server may close the connection).
				p.reconnectPool()
			}
		} else {
			client, _ := p.vanillaCluster.Client(clusterAddr[slotP])
			cmdSlots[slotP], timesSlots[slotP], _ = sendFlatCmd(p, client, cmdType, cmdQueryId, cmd, docFields, bytelen, cmdSlots[slotP], replies, timesSlots[slotP])
		}
	}
	p.wg.Done()
}

func getRxLen(v interface{}) (res uint64) {
	res = 0
	switch x := v.(type) {
	case []string:
		for _, i := range x {
			res += uint64(len(i))
		}
	case string:
		res += uint64(len(x))
	default:
		res = 0
	}
	return
}

func sendFlatCmd(p *processor, client radix.Client, cmdType, cmdQueryId, cmd string, docfields []string, txBytesCount uint64, cmds []radix.CmdAction, replies []interface{}, times []time.Time) ([]radix.CmdAction, []time.Time, bool) {
	var err error = nil
	var rcv interface{}
	rxBytesCount := uint64(0)
	var radixFlatCmd = radix.Cmd(rcv, cmd, docfields...)
	cmds = append(cmds, radixFlatCmd)
	replies = append(replies, rcv)
	start := time.Now()
	times = append(times, start)
	key := ""
	if len(docfields) > 0 {
		key = docfields[0]
	}
	cmds, times, hadError := sendIfRequired(p, client, cmdType, cmdQueryId, cmd, key, cmds, err, times, rxBytesCount, replies, txBytesCount)
	return cmds, times, hadError
}

func sendIfRequired(p *processor, client radix.Client, cmdType string, cmdQueryId string, redisCmd string, redisKey string, cmds []radix.CmdAction, err error, times []time.Time, rxBytesCount uint64, replies []interface{}, txBytesCount uint64) ([]radix.CmdAction, []time.Time, bool) {
	cmdLen := len(cmds)
	hadError := false
	if cmdLen >= pipeline {
		if cmdLen == 1 {
			// if pipeline is 1 no need to pipeline
			err = client.Do(cmds[0])
		} else {
			err = client.Do(radix.Pipeline(cmds...))
		}
		endT := time.Now()
		isTimeout := false
		if err != nil {
			hadError = true

			// Always log the error
			// For read commands, log full command details; for writes, log only a summary to avoid huge log lines
			if cmdType == "READ" || cmdType == "READ_CURSOR" {
				if continueOnErr {
					log.Println(fmt.Sprintf("Received an error with the following command(s): %v, error: %v", cmds, err))
				} else {
					log.Fatal(fmt.Sprintf("Fatal error with the following command(s): %v, error: %v", cmds, err))
				}
			} else {
				if continueOnErr {
					log.Println(fmt.Sprintf("Received an error with %s command: %s %s (%d command(s) in pipeline), error: %v", cmdType, redisCmd, redisKey, len(cmds), err))
				} else {
					log.Fatal(fmt.Sprintf("Fatal error with %s command: %s %s (%d command(s) in pipeline), error: %v", cmdType, redisCmd, redisKey, len(cmds), err))
				}
			}

			// Log additional timeout-specific message if it's a timeout
			if strings.Contains(err.Error(), "i/o timeout") {
				isTimeout = true
				if cmdType == "READ" || cmdType == "READ_CURSOR" {
					log.Println(fmt.Sprintf("Timeout occurred with the following command(s): %v, continuing execution...", cmds))
				} else {
					log.Println(fmt.Sprintf("Timeout occurred with %s command: %s %s (%d command(s) in pipeline), continuing execution...", cmdType, redisCmd, redisKey, len(cmds)))
				}
			}
		}
		for pos, t := range times {
			duration := endT.Sub(t)
			took := uint64(duration.Microseconds())
			rcv := replies[pos]
			rxBytesCount += getRxLen(rcv)
			stat := benchmark_runner.NewStat().AddEntry([]byte(cmdType), []byte(cmdQueryId), uint64(t.Unix()), took, hadError, isTimeout, txBytesCount, rxBytesCount)
			p.cmdChan <- *stat
		}
		cmds = nil
		cmds = make([]radix.CmdAction, 0, 0)
		times = nil
		times = make([]time.Time, 0, 0)
	}
	return cmds, times, hadError
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
		// Subtract the base64 shrink so byte accounting reflects the decoded
		// bytes actually sent to Redis, not the larger base64 text in the row.
		bytelen = uint64(len(row)) - uint64(len(cmdType)) - shrink
	} else {
		err = fmt.Errorf("input string does not have the minimum required size of 2: %s", row)
	}

	return
}
