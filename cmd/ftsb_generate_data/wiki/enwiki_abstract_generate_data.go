package wiki

import (
	"encoding/xml"
	"fmt"
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_data/common"
	"github.com/RediSearch/redisearch-go/redisearch"
	"github.com/google/uuid"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
)

// WikiAbstractSimulatorConfig is used to create a FTSSimulator.
type WikiAbstractSimulatorConfig commonFTSSimulatorConfig

// NewSimulator produces a Simulator that conforms to the given SimulatorConfig over the specified interval
func (c *WikiAbstractSimulatorConfig) NewSimulator(limit uint64, inputFilename string, debug int) common.Simulator {
	//https://github.com/RediSearch/RediSearch/issues/307
	//prevent field tokenization ,.<>{}[]"':;!@#$%^&*()-+=~
	field_tokenization := ",.<>{}[]\"':;!@#$%^&*()-+=~"
	var documents []redisearch.Document
	xmlFile, _ := os.Open(inputFilename)
	dec := xml.NewDecoder(xmlFile)

	maxPoints := limit
	tok, err := dec.RawToken()

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
			if name == "title" || name == "url" || name == "abstract" {
				props[name] = currentText
			} else if name == "doc" {
				u2, _ := uuid.NewRandom()
				id := u2.String() + "-" + path.Base(props["url"])
				props["title"] = strings.TrimPrefix(strings.TrimSpace(props["title"]), "Wikipedia: ")
				props["abstract"] = strings.TrimSpace(props["abstract"])
				props["url"] = strings.TrimSpace(props["url"])

				props["title"] = strings.ReplaceAll(props["title"], "=", "=")
				props["abstract"] = strings.ReplaceAll(props["abstract"], "=", "=")
				props["url"] = strings.ReplaceAll(props["url"], "=", "=")

				props["title"] = strings.ReplaceAll(props["title"], "\"", "\\\"")
				props["abstract"] = strings.ReplaceAll(props["abstract"], "\"", "\\\"")
				props["url"] = strings.ReplaceAll(props["url"], "\"", "\\\"")

				for _, char := range field_tokenization {
					props["abstract"] = strings.ReplaceAll(props["abstract"], string(char), string("\\"+string(char)))
					props["url"] = strings.ReplaceAll(props["url"], string(char), string("\\"+string(char)))
					props["title"] = strings.ReplaceAll(props["title"], string(char), string("\\"+string(char)))
				}

				if debug > 1 {
					fmt.Fprintln(os.Stderr, "At document "+id)
				}
				doc := redisearch.NewDocument(id, 1.0).
					Set("Title", props["title"]).
					Set("Url", props["url"]).
					Set("Abstract", props["abstract"])
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
