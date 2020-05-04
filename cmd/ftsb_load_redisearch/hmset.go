package main

import (
	"fmt"
	"log"
	"os"
	"strings"
)

func rowToHash(row string) (cmd string, args []string, bytelen int, err error) {
	cmd = "hmset"
	bytelen = len(cmd)
	args = make([]string, 0)
	if debug > 0 {
		fmt.Fprintln(os.Stderr, "converting row to hash "+row)
	}
	fieldSizesStr := strings.Split(row, ",")
	// we need at least the id and score
	if len(fieldSizesStr) >= 2 {
		documentId := loader.DatabaseName() + "-" + fieldSizesStr[0]
		bytelen += len(documentId)
		//documentScore, _ := strconv.ParseFloat(fieldSizesStr[1], 64)
		args = append(args, documentId)
		//doc := redisearch.NewDocument(documentId, float32(documentScore))

		for _, keyValuePair := range fieldSizesStr[2:] {
			pair := strings.Split(keyValuePair, "=")
			if len(pair) == 2 {
				if debug > 0 {
					fmt.Fprintln(os.Stderr, "On doc "+documentId+" adding field with NAME "+pair[0]+" and VALUE "+pair[1])
				}
				bytelen += len(pair[0])
				bytelen += len(pair[1])
				args = append(args, pair[0], pair[1])
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
	}
	return
}
