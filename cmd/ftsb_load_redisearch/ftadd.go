package main

import (
	"fmt"
	"github.com/RediSearch/redisearch-go/redisearch"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
)

//, client* redisearch.Client,  pipelineSize int, documents []redisearch.Document
func rowToRSDocument(row string) (document *redisearch.Document) {
	if debug > 0 {
		fmt.Fprintln(os.Stderr, "converting row to rediSearch Document "+row)
	}
	fieldSizesStr := strings.Split(row, ",")
	// we need at least the id and score
	if len(fieldSizesStr) >= 2 {
		documentId := loader.DatabaseName() + "-" + fieldSizesStr[0]
		documentScore, _ := strconv.ParseFloat(fieldSizesStr[1], 64)
		doc := redisearch.NewDocument(documentId, float32(documentScore))

		for _, keyValuePair := range fieldSizesStr[2:] {
			pair := strings.Split(keyValuePair, "=")
			if len(pair) == 2 {
				if debug > 0 {
					fmt.Fprintln(os.Stderr, "On doc "+documentId+" adding field with NAME "+pair[0]+" and VALUE "+pair[1])
				}
				doc.Set(pair[0], pair[1])
			} else {
				if debug > 0 {
					fmt.Fprintf(os.Stderr, "On doc "+documentId+" len(pair)=%d", len(pair))
				}
				log.Fatalf("keyValuePair pair size != 2 . Got " + keyValuePair)
			}
		}
		if debug > 0 {
			fmt.Fprintln(os.Stderr, "Doc "+documentId)
		}
		return &doc
	}
	return document
}

func ftaddInsertWorkflow(p *processor, pipeline uint64, doc *redisearch.Document, totalBytes uint64, deleteUpperLimit float64, updateUpperLimit float64, pipelinePos uint64, indexingOpts redisearch.IndexingOptions, documents []redisearch.Document, insertCount uint64, updateOpts redisearch.IndexingOptions) (uint64, uint64, []redisearch.Document, uint64) {
	documentPayload := uint64((*doc).EstimateSize())
	totalBytes += documentPayload
	(*doc).EstimateSize()
	val := rand.Float64()
	// DELETE
	// TODO:
	// UPDATE
	// only possible if we already have something to update
	if val >= deleteUpperLimit && val < updateUpperLimit && (len(p.insertedDocIds) > 0) {
		p.insertedDocIds = append(p.insertedDocIds, doc.Id)
		idToUdpdate := p.insertedDocIds[rand.Intn(len(p.insertedDocIds))]
		doc.Id = idToUdpdate
		// make sure we flush the pipeline prior than updating
		if pipelinePos > 0 {
			// Index the document. The API accepts multiple documents at a time
			processorIndexInsertDocuments(p, indexingOpts, documents, totalBytes, insertCount)
			documents, insertCount, pipelinePos, totalBytes = LocalCountersReset()
		}
		processorIndexUpdateDocument(p, updateOpts, doc, totalBytes)
		documents, insertCount, pipelinePos, totalBytes = LocalCountersReset()
		// INSERT
	} else {
		documents = append(documents, *doc)
		p.insertedDocIds = append(p.insertedDocIds, doc.Id)
		insertCount++
		pipelinePos++
	}
	if pipelinePos%pipeline == 0 && len(documents) > 0 {
		// Index the document. The API accepts multiple documents at a time
		processorIndexInsertDocuments(p, indexingOpts, documents, totalBytes, insertCount)
		documents, insertCount, pipelinePos, totalBytes = LocalCountersReset()
	}
	return totalBytes, pipelinePos, documents, insertCount
}
