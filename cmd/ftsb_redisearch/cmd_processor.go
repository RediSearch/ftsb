package main

import (
	"encoding/csv"
	"fmt"
	"github.com/RediSearch/ftsb/benchmark_runner"
	radix "github.com/mediocregopher/radix/v3"
	"golang.org/x/time/rate"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
)

type processor struct {
	rows           chan string
	cmdChan        chan benchmark_runner.Stat
	wg             *sync.WaitGroup
	vanillaClient  *radix.Pool
	vanillaCluster *radix.Cluster
	clusterTopo    radix.ClusterTopo
}

func (p *processor) Init(workerNumber int, _ bool, totalWorkers int) {
	var err error = nil
	opts := make([]radix.DialOpt, 0)
	if password != "" {
		opts = append(opts, radix.DialAuthPass(password))
	}
	opts = append(opts, radix.DialTimeout(time.Second*600))

	customConnFunc := func(network, addr string) (radix.Conn, error) {
		return radix.Dial(network, addr, opts...,
		)
	}

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
		p.vanillaClient, err = radix.NewPool("tcp", host, 1, radix.PoolConnFunc(customConnFunc), radix.PoolPipelineWindow(0, 0), radix.PoolPingInterval(1*time.Hour))
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
		// start at a random slot between 0 and clusterAddrLen
		slotP = rand.Intn(clusterAddrLen)
	}

	for row := range p.rows {
		cmdType, cmdQueryId, keyPos, cmd, key, clusterSlot, docFields, bytelen, _ := preProcessCmd(row)

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
			cmdSlots[slotP], timesSlots[slotP] = sendFlatCmd(p, p.vanillaClient, cmdType, cmdQueryId, cmd, docFields, bytelen, cmdSlots[slotP], replies, timesSlots[slotP])
		} else {
			client, _ := p.vanillaCluster.Client(clusterAddr[slotP])
			cmdSlots[slotP], timesSlots[slotP] = sendFlatCmd(p, client, cmdType, cmdQueryId, cmd, docFields, bytelen, cmdSlots[slotP], replies, timesSlots[slotP])
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

func sendFlatCmd(p *processor, client radix.Client, cmdType, cmdQueryId, cmd string, docfields []string, txBytesCount uint64, cmds []radix.CmdAction, replies []interface{}, times []time.Time) ([]radix.CmdAction, []time.Time) {
	var err error = nil
	var rcv interface{}
	rxBytesCount := uint64(0)
	var radixFlatCmd = radix.Cmd(rcv, cmd, docfields...)
	cmds = append(cmds, radixFlatCmd)
	replies = append(replies, rcv)
	start := time.Now()
	times = append(times, start)
	cmds, times = sendIfRequired(p, client, cmdType, cmdQueryId, cmds, err, times, rxBytesCount, replies, txBytesCount)
	return cmds, times
}

func sendIfRequired(p *processor, client radix.Client, cmdType string, cmdQueryId string, cmds []radix.CmdAction, err error, times []time.Time, rxBytesCount uint64, replies []interface{}, txBytesCount uint64) ([]radix.CmdAction, []time.Time) {
	cmdLen := len(cmds)
	if cmdLen >= pipeline {
		if cmdLen == 1 {
			// if pipeline is 1 no need to pipeline
			err = client.Do(cmds[0])
		} else {
			err = client.Do(radix.Pipeline(cmds...))
		}
		endT := time.Now()
		if err != nil {
			if continueOnErr {
				if debug > 0 {
					log.Println(fmt.Sprintf("Received an error with the following command(s): %v, error: %v", cmds, err))
				}
			} else {
				log.Fatal(err)
			}
		}
		for pos, t := range times {
			duration := endT.Sub(t)
			took := uint64(duration.Microseconds())
			rcv := replies[pos]
			rxBytesCount += getRxLen(rcv)
			stat := benchmark_runner.NewStat().AddEntry([]byte(cmdType), []byte(cmdQueryId), uint64(t.Unix()), took, false, false, txBytesCount, rxBytesCount)
			p.cmdChan <- *stat
		}
		cmds = nil
		cmds = make([]radix.CmdAction, 0, 0)
		times = nil
		times = make([]time.Time, 0, 0)
	}
	return cmds, times
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
		if len(argsStr) > 4 {
			args = argsStr[4:]
			key = argsStr[keyPos]
		}
		if initialPos >= 0 {
			clusterSlot = int(radix.ClusterSlot([]byte(key)))
		}
		bytelen = uint64(len(row)) - uint64(len(cmdType))
	} else {
		err = fmt.Errorf("input string does not have the minimum required size of 2: %s", row)
	}

	return
}
