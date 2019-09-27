package wiki

import (
	"encoding/xml"
	"fmt"
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_data/common"
	"github.com/RediSearch/redisearch-go/redisearch"
	"github.com/google/uuid"
	"io"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

// WikiAbstractSimulatorConfig is used to create a FTSSimulator.
type WikiPagesSimulatorConfig commonFTSSimulatorConfig

// NewSimulator produces a Simulator that conforms to the given SimulatorConfig over the specified interval
func (c *WikiPagesSimulatorConfig) NewSimulator(limit uint64, inputFilename string, debug int) common.Simulator {
	//https://github.com/RediSearch/RediSearch/issues/307
	//prevent field tokenization ,.<>{}[]"':;!@#$%^&*()-+=~
	field_tokenization := ",.<>{}[]\"':;!@#$%^&*()-+=~"
	var documents []redisearch.Document
	xmlFile, _ := os.Open(inputFilename)
	dec := xml.NewDecoder(xmlFile)

	maxPoints := limit
	tok, err := dec.RawToken()
	layout := "2006-01-02T15:04:05Z"

	props := map[string]string{}
	var currentText string
	if debug > 0 {
		fmt.Fprintln(os.Stderr, "started reading "+inputFilename)
	}
	docCount := uint64(0)
	for err != io.EOF && (docCount < limit || limit == 0) {

		switch t := tok.(type) {

		case xml.CharData:
			if len(t) > 1 {
				currentText += string(t)
			}


		case xml.EndElement:
			name := t.Name.Local
			if name == "title" ||
				name == "ns" ||
				name == "id" ||
				name == "parentid" ||
				name == "username" ||
				name == "comment" {
				props[name] = currentText
			} else if name == "timestamp" {
				currentText = strings.TrimSpace(currentText)
				t, _ := time.Parse(layout,currentText)
				props[name] = fmt.Sprintf("%d", t.Unix())
			} else if name == "page" {
				u2, _ := uuid.NewRandom()
				for key, value := range props {
					props[key] = strings.TrimSpace(value)
					props[key] = strings.ReplaceAll(props[key], "=", "")
					props[key] = strings.ReplaceAll(props[key], ",", "")
					props[key] = strings.ReplaceAll(props[key], "\"", "\\\"")
					for _, char := range field_tokenization {
						props[key] = strings.ReplaceAll(props[key], string(char), string("\\"+string(char)))
					}
				}

				if debug > 1 {
					fmt.Fprintln(os.Stderr, "At document "+props["id"])
				}
				id := u2.String() + "-" + props["id"]
				doc := redisearch.NewDocument(id, 1.0).
					Set("TITLE", props["title"]).
					Set("NAMESPACE", props["ns"]).
					Set("ID", props["id"]).
					Set("PARENT_REVISION_ID", props["parentid"]).
					Set("CURRENT_REVISION_TIMESTAMP", props["timestamp"]).
					Set("CURRENT_REVISION_ID", props["id"]).
					Set("CURRENT_REVISION_EDITOR_USERNAME", props["username"]).
					Set("CURRENT_REVISION_EDITOR_IP", fmt.Sprintf("%d.%d.%d.%d", rand.Intn(256), rand.Intn(256), rand.Intn(256), rand.Intn(256))).
					Set("CURRENT_REVISION_EDITOR_USERID", props["id"]).
					Set("CURRENT_REVISION_EDITOR_COMMENT", props["comment"]).
					Set("CURRENT_REVISION_CONTENT_LENGTH", len(props["comment"]))
				documents = append(documents, doc)

				props = map[string]string{}
				docCount++
				if debug > 0 {
					if docCount%1000 == 0 {
						fmt.Fprintln(os.Stderr, "At document "+strconv.Itoa(int(docCount)))
					}
				}

			}
			currentText = ""
		}

		tok, err = dec.RawToken()

	}

	if debug > 0 {
		fmt.Fprintln(os.Stderr, "finished reading "+inputFilename)
	}

	maxPoints = uint64(len(documents))
	if limit > 0 && limit < uint64(len(documents)) {
		// Set specified points number limit
		maxPoints = limit
	}
	sim := &FTSSimulator{&commonFTSSimulator{
		madeDocuments: 0,
		maxDocuments:  maxPoints,

		recordIndex: 0,
		records:     documents,
	}}

	return sim
}
