package wiki

import (
	"math/rand"
)

func NewCore(pagesEditors []string, seed int64, inferiorLimit int64, superiorLimit int64) *Core {
	rand.Seed(seed)
	return &Core{
		PagesEditors:                  pagesEditors,
		PagesEditorsIndexPosition:     0,
		PagesEditorsQueryIndex:        uint64(len(pagesEditors)),
		SuperiorTimeLimitPagesRecords: superiorLimit,
		InferiorTimeLimitPagesRecords: inferiorLimit,
		MaxRandomInterval:             superiorLimit - inferiorLimit,
	}
}

func NewCoreFromAbstract(OneWord []string, TwoWord [][]string, OneWordSpellCheck []string, OneWordSpellCheckDistance []int) *Core {
	return &Core{
		OneWordQueries:            OneWord,
		OneWordQueryIndexPosition: 0,
		OneWordQueryIndex:         uint64(len(OneWord)),

		TwoWordQueries:            TwoWord,
		TwoWordQueryIndexPosition: 0,
		TwoWordQueryIndex:         uint64(len(TwoWord)),

		OneWordSpellCheckQueries:            OneWordSpellCheck,
		OneWordSpellCheckQueriesDistance:    OneWordSpellCheckDistance,
		OneWordSpellCheckQueryIndexPosition: 0,
		OneWordSpellCheckQueryIndex:         uint64(len(OneWordSpellCheck)),
	}
}

// Core is the common component of all generators for all systems
type Core struct {
	// Abstracts Use Case
	TwoWordQueries            [][]string
	TwoWordQueryIndexPosition uint64
	TwoWordQueryIndex         uint64

	OneWordQueries            []string
	OneWordQueryIndexPosition uint64
	OneWordQueryIndex         uint64

	OneWordSpellCheckQueries            []string
	OneWordSpellCheckQueriesDistance    []int
	OneWordSpellCheckQueryIndexPosition uint64
	OneWordSpellCheckQueryIndex         uint64

	// Pages Use Case
	PagesEditors                  []string
	PagesEditorsIndexPosition     uint64
	PagesEditorsQueryIndex        uint64
	SuperiorTimeLimitPagesRecords int64
	InferiorTimeLimitPagesRecords int64
	MaxRandomInterval             int64
}
