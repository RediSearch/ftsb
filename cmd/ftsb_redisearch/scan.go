package main

import (
	"bufio"
	"github.com/RediSearch/ftsb/benchmark_runner"
	"log"
	"sync"
)

type decoder struct {
	scanner *bufio.Scanner
}

// Reads and returns a text line that encodes a databuild point for a specif field name.
// Since scanning happens in a single thread, we hold off on transforming it
// to an INSERT statement until it's being processed concurrently by a worker.
func (d *decoder) Decode(_ *bufio.Reader) *benchmark_runner.DocHolder {
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
		return nil
	} else if !ok {
		log.Fatalf("scan error: %v", d.scanner.Err())
	}
	return benchmark_runner.NewDocument(d.scanner.Text())
}

type eventsBatch struct {
	rows []string
}

func (eb *eventsBatch) Len() int {
	return len(eb.rows)
}

func (eb *eventsBatch) Append(item *benchmark_runner.DocHolder) {
	that := item.Data.(string)
	eb.rows = append(eb.rows, that)
}

var ePool = &sync.Pool{New: func() interface{} { return &eventsBatch{rows: []string{}} }}

type factory struct{}

func (f *factory) New() benchmark_runner.Batch {
	return ePool.Get().(*eventsBatch)
}
