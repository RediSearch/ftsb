package main

import (
	"bufio"
	"log"
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

func sendRedisCommand(line string, conn redis.Conn) {
	t := strings.Split(line, " ")
	s := redis.Args{}.AddFlat(t[1:])
	//fmt.Println(line)
	//fmt.Println(s)
	//err := conn.Do(t[0],s...)
	_, err := conn.Do(t[0],s...)
	//err := conn.Send(t[0], s...)
	if err != nil {
		log.Fatalf("sendRedisCommand %s failed: %s\n", t[0], err)
	}
}

func sendRedisFlush(count uint64, conn redis.Conn) (metrics uint64, err error) {
	metrics = uint64(0)
	err = conn.Flush()
	if err != nil {
		return
	}
	for i := uint64(0); i < count; i++ {
		rep, err := conn.Receive()
		if err != nil {
			return 0, err
		}
		arr, err := redis.Values(rep, nil)
		if err != nil {
			metrics += uint64(len(arr))
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
