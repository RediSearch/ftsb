package main

import (
	"bufio"
	"log"
	"strconv"
	"strings"
	"sync"

	"github.com/filipecosta90/ftsb/load"
	"github.com/gomodule/redigo/redis"
)

type decoder struct {
	scanner *bufio.Scanner
}

// Reads and returns a text line that encodes a data point for a specif field name.
// Since scanning happens in a single thread, we hold off on transforming it
// to an INSERT statement until it's being processed concurrently by a worker.
func (d *decoder) Decode(_ *bufio.Reader) *load.Point {
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
		return nil
	} else if !ok {
		log.Fatalf("scan error: %v", d.scanner.Err())
	}
	return load.NewPoint(d.scanner.Text())
}

func sendRedisCommand(row string, conn redis.Conn) {
	nFieldsStr := strings.SplitN(row, ",", 2)
	if len(nFieldsStr) != 2 {
		log.Fatalf("row does not have the correct format( len %d ) %s failed\n", len(nFieldsStr), row)
	}
	nFields, _ := strconv.Atoi(nFieldsStr[0])

	fieldSizesStr := strings.SplitN(nFieldsStr[1], ",", nFields+1)
	ftsRow := fieldSizesStr[nFields]
	var cmdArgs []string

	previousPos := 0
	fieldLen := 0
	for i := 0; i < nFields; i++ {
		fieldLen, _ = strconv.Atoi(fieldSizesStr[i])
		cmdArgs = append(cmdArgs, ftsRow[previousPos:(previousPos + fieldLen)])
		previousPos = previousPos + fieldLen

	}

	s := redis.Args{}.AddFlat(cmdArgs)
	//metricValue := uint64(1)

	err := conn.Send("FT.ADD", s...)
	////err := conn.Send(t[0], s...)
	if err != nil {
		log.Fatalf("FT.ADD %s failed: %s\n", s, err)
		//	metricValue = uint64(0)
	}
}

func sendRedisFlush(count uint64, conn redis.Conn) (metrics uint64, err error) {
	metrics = uint64(0)
	err = conn.Flush()
	if err != nil {
		log.Fatalf("Error on flush \n", err)
	}

	for i := uint64(0); i < count; i++ {
		_, err := conn.Receive()
		if err == nil {
			metrics += 1
		}
	}
	return metrics, nil
}

type eventsBatch struct {
	rows []string
}

func (eb *eventsBatch) Len() int {
	return len(eb.rows)
}

func (eb *eventsBatch) Append(item *load.Point) {
	that := item.Data.(string)
	eb.rows = append(eb.rows, that)
}

var ePool = &sync.Pool{New: func() interface{} { return &eventsBatch{rows: []string{}} }}

type factory struct{}

func (f *factory) New() load.Batch {
	return ePool.Get().(*eventsBatch)
}
