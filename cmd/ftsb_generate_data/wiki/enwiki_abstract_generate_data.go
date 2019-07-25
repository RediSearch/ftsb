package wiki

import (
	"encoding/xml"
	"fmt"
	"github.com/filipecosta90/ftsb/cmd/ftsb_generate_data/common"
	"github.com/filipecosta90/ftsb/cmd/ftsb_generate_data/serialize"
	"github.com/google/uuid"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
)

// A FTSSimulator generates data similar to telemetry from Telegraf for only CPU metrics.
// It fulfills the Simulator interface.
type FTSSimulator struct {
	*commonFTSSimulator
}

// Next advances a Document to the next state in the generator.
func (d *FTSSimulator) Next(p *serialize.Document) bool {
	// Switch to the next document
	if d.recordIndex >= uint64(len(d.records)) {
		d.recordIndex = 0
	}
	return d.populateDocument(p)
}

func (s *FTSSimulator) populateDocument(p *serialize.Document) bool {
	record := &s.records[s.recordIndex]

	p.Id = record.Id
	p.Title = record.Title
	p.Url = record.Url
	p.Abstract = record.Abstract

	ret := s.recordIndex < uint64(len(s.records))
	s.recordIndex = s.recordIndex + 1
	s.madeDocuments = s.madeDocuments + 1
	return ret
}

// FTSSimulatorConfig is used to create a FTSSimulator.
type FTSSimulatorConfig commonFTSSimulatorConfig

// NewSimulator produces a Simulator that conforms to the given SimulatorConfig over the specified interval
func (c *FTSSimulatorConfig) NewSimulator(limit uint64, inputFilename string, debug int) common.Simulator {
	//https://github.com/RediSearch/RediSearch/issues/307
	//prevent field tokenization ,.<>{}[]"':;!@#$%^&*()-+=~
	field_tokenization := ",.<>{}[]\"':;!@#$%^&*()-+=~"
	var documents []serialize.Document
	xmlFile, _ := os.Open(inputFilename)
	dec := xml.NewDecoder(xmlFile)

	maxPoints := limit
	tok, err := dec.RawToken()

	props := map[string]string{}
	var currentText string
	if debug > 0 {
		fmt.Fprintln(os.Stderr, "started reading " + inputFilename)
	}
	docCount := uint64(0)
	for err != io.EOF && (docCount < limit || limit == 0 ) {

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
				props["title"] = strings.ReplaceAll(props["title"], "\"", "\\\"")
				props["abstract"] = strings.ReplaceAll(props["abstract"], "\"", "\\\"")
				props["url"] = strings.ReplaceAll(props["url"], "\"", "\\\"")

				for _, char := range field_tokenization {
					props["abstract"] = strings.ReplaceAll(props["abstract"], string(char), string("\\"+string(char)))
					props["url"] = strings.ReplaceAll(props["url"], string(char), string("\\"+string(char)))
					props["title"] = strings.ReplaceAll(props["title"], string(char), string("\\"+string(char)))
				}

				if debug > 1 {
						fmt.Fprintln(os.Stderr, "At document " + id )
				}


				documents = append(documents, serialize.Document{[]byte(id), []byte( props["title"]), []byte(props["url"]), []byte( props["abstract"])})
				props = map[string]string{}
				docCount++
				if debug > 0 {
					if docCount % 1000 == 0 {
						fmt.Fprintln(os.Stderr, "At document " + strconv.Itoa(int(docCount)))
					}
				}


			}
			currentText = ""
		}

		tok, err = dec.RawToken()

	}

	if debug > 0 {
		fmt.Fprintln(os.Stderr, "finished reading " + inputFilename)
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
