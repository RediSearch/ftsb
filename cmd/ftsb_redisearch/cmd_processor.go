package main

import (
	"encoding/csv"
	"fmt"
	"github.com/RediSearch/ftsb/benchmark_runner"
	"github.com/mediocregopher/radix/v3"
	"log"
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
	if clusterMode {
		poolFunc := func(network, addr string) (radix.Client, error) {
			return radix.NewPool(network, addr, 1, radix.PoolPipelineWindow(time.Duration(0), 0))
		}
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
		p.vanillaClient, err = radix.NewPool("tcp", host, 1, radix.PoolPipelineWindow(0, 0), radix.PoolPingInterval(1*time.Hour))
		if err != nil {
			log.Fatalf("Error preparing for redisearch ingestion, while creating new pool. error = %v", err)
		}
	}
}

func connectionProcessor(p *processor) {
	cmdSlots := make([][]radix.CmdAction, 0, 0)
	timesSlots := make([][]time.Time, 0, 0)
	clusterSlots := make([][2]uint16, 0, 0)
	clusterAddr := make([]string, 0, 0)

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
	}
	for row := range p.rows {
		cmdType, cmdQueryId, keyPos, cmd, key, clusterSlot, docFields, bytelen, _ := preProcessCmd(row)
		for i, sArr := range clusterSlots {
			if clusterSlot >= sArr[0] && clusterSlot < sArr[1] {
				slotP = i
			}
		}
		if debug > 2 {
			fmt.Println(keyPos, key, clusterSlot, cmd, slotP, clusterSlots)
		}
		if !clusterMode {
			cmdSlots[slotP], timesSlots[slotP] = sendFlatCmd(p, p.vanillaClient, cmdType, cmdQueryId, cmd, docFields, bytelen, 1, cmdSlots[slotP], timesSlots[slotP])
		} else {
			client, _ := p.vanillaCluster.Client(clusterAddr[slotP])
			cmdSlots[slotP], timesSlots[slotP] = sendFlatCmd(p, client, cmdType, cmdQueryId, cmd, docFields, bytelen, 1, cmdSlots[slotP], timesSlots[slotP])
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

func sendFlatCmd(p *processor, client radix.Client, cmdType, cmdQueryId, cmd string, docfields []string, txBytesCount, insertCount uint64, cmds []radix.CmdAction, times []time.Time) ([]radix.CmdAction, []time.Time) {
	var err error = nil
	var rcv interface{}
	rxBytesCount := uint64(0)
	var radixFlatCmd = radix.Cmd(nil, cmd, docfields...)
	cmds = append(cmds, radixFlatCmd)
	start := time.Now()
	times = append(times, start)
	cmds, times = sendIfRequired(p, client, cmdType, cmdQueryId, cmds, err, times, rxBytesCount, rcv, txBytesCount)
	return cmds, times
}

func sendIfRequired(p *processor, client radix.Client, cmdType string, cmdQueryId string, cmds []radix.CmdAction, err error, times []time.Time, rxBytesCount uint64, rcv interface{}, txBytesCount uint64) ([]radix.CmdAction, []time.Time) {
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
		for _, t := range times {
			duration := endT.Sub(t)
			took := uint64(duration.Microseconds())
			rxBytesCount += getRxLen(rcv)
			stat := benchmark_runner.NewStat().AddEntry([]byte(cmdType), []byte(cmdQueryId), took, false, false, txBytesCount, rxBytesCount)
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
func (p *processor) ProcessBatch(b benchmark_runner.Batch, doLoad bool) (outstat benchmark_runner.Stat) {
	outstat = *benchmark_runner.NewStat()
	events := b.(*eventsBatch)
	rowCnt := uint64(len(events.rows))
	if doLoad {
		buflen := rowCnt + 1

		p.cmdChan = make(chan benchmark_runner.Stat, buflen)
		p.wg = &sync.WaitGroup{}
		p.rows = make(chan string, buflen)
		p.wg.Add(1)
		go connectionProcessor(p)
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

func preProcessCmd(row string) (cmdType string, cmdQueryId string, keyPos int, cmd string, key string, clusterSlot uint16, args []string, bytelen uint64, err error) {
	reader := csv.NewReader(strings.NewReader(row))
	argsStr, err := reader.Read()
	if err != nil {
		return
	}

	// we need at least the cmdType and command
	if len(argsStr) >= 3 {
		cmdType = argsStr[0]
		cmdQueryId = argsStr[1]
		keyPos, _ = strconv.Atoi(argsStr[2])
		keyPos = keyPos+3
		cmd = argsStr[3]
		if len(argsStr) > 4 {
			args = argsStr[4:]
			key = argsStr[keyPos]
			clusterSlot = radix.ClusterSlot([]byte(key))
		}
		bytelen = uint64(len(row)) - uint64(len(cmdType))
	} else {
		err = fmt.Errorf("input string does not have the minimum required size of 2: %s", row)
	}

	return
}
