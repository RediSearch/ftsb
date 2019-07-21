package query

import (
	"fmt"
	"sync"
)

// RediSearch encodes a RediSearch request. This will be serialized for use
// by the ftsb_run_queries_redisearch program.
type RediSearch struct {
	HumanLabel       []byte
	HumanDescription []byte

	RedisQuery []byte
	id         uint64
}

// RediSearchPool is a sync.Pool of RediSearch Query types
var RediSearchPool = sync.Pool{
	New: func() interface{} {
		return &RediSearch{
			HumanLabel:       make([]byte, 0, 1024),
			HumanDescription: make([]byte, 0, 1024),
			RedisQuery:       make([]byte, 0, 1024),
		}
	},
}

// NewRediSearch returns a new RediSearch Query instance
func NewRediSearch() *RediSearch {
	return RediSearchPool.Get().(*RediSearch)
}

// GetID returns the ID of this Query
func (q *RediSearch) GetID() uint64 {
	return q.id
}

// SetID sets the ID for this Query
func (q *RediSearch) SetID(n uint64) {
	q.id = n
}

// String produces a debug-ready description of a Query.
func (q *RediSearch) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s, Query: %s", q.HumanLabel, q.HumanDescription, q.RedisQuery)
}

// HumanLabelName returns the human readable name of this Query
func (q *RediSearch) HumanLabelName() []byte {
	return q.HumanLabel
}

// HumanDescriptionName returns the human readable description of this Query
func (q *RediSearch) HumanDescriptionName() []byte {
	return q.HumanDescription
}

// Release resets and returns this Query to its pool
func (q *RediSearch) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]
	q.id = 0

	q.RedisQuery = q.RedisQuery[:0]

	RediSearchPool.Put(q)
}
