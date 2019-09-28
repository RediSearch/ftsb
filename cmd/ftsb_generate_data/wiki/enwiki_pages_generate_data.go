package wiki

import (
	"encoding/xml"
	"fmt"
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_data/common"
	"github.com/RediSearch/redisearch-go/redisearch"
	"github.com/google/uuid"
	"io"
	"log"
	"math/rand"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// WikiAbstractSimulatorConfig is used to create a FTSSimulator.
type WikiPagesSimulatorConfig commonFTSSimulatorConfig

// NewSimulator produces a Simulator that conforms to the given SimulatorConfig over the specified interval
func (c *WikiPagesSimulatorConfig) NewSimulator(limit uint64, inputFilename string, debug int) common.Simulator {
	documents, _, maxPoints := WikiPagesParseXml(inputFilename, limit, debug, []string{}, 0)
	sim := &FTSSimulator{&commonFTSSimulator{
		madeDocuments: 0,
		maxDocuments:  maxPoints,

		recordIndex: 0,
		records:     documents,
	}}

	return sim
}

// NewWikiAbrastractReader returns a new Core for the given input filename, seed, and maxQueries
func NewWikiPagesReader(filename string, stopwordsbl []string, seed int64, maxQueries int, debug int ) *Core {
	_, editors, _ := WikiPagesParseXml(filename, uint64(maxQueries), debug, []string{}, seed)
	return NewCore(editors)
}

func WikiPagesParseXml(inputFilename string, limit uint64, debug int, stopwordsbl []string, seed int64) ([]redisearch.Document, []string, uint64) {
	//https://github.com/RediSearch/RediSearch/issues/307
	//prevent field tokenization ,.<>{}[]"':;!@#$%^&*()-+=~
	field_tokenization := ",.<>{}[]\"':;!@#$%^&*()-+=~"
	rand.Seed(seed)
	// Make a Regex to say we only want letters and numbers
	reg, err := regexp.Compile("[^a-zA-Z0-9 ]+")
	if err != nil {
		log.Fatal(err)
	}

	var editors []string
	var documents []redisearch.Document
	xmlFile, _ := os.Open(inputFilename)
	dec := xml.NewDecoder(xmlFile)
	maxPoints := limit
	tok, err := dec.RawToken()
	layout := "2006-01-02T15:04:05Z"
	props := map[string]string{}
	var currentText string
	if debug > 0 {
		fmt.Fprintln(os.Stderr, "pages started reading "+inputFilename)
	}
	docCount := uint64(0)
	for err != io.EOF && (docCount < limit || limit == 0) {
		documents, editors, docCount, err  = ProcessWikiPagesXml(reg, tok, currentText, props, layout, field_tokenization, debug, documents, editors, docCount, err, dec)
	}
	if debug > 0 {
		fmt.Fprintln(os.Stderr, "finished reading "+inputFilename)
	}
	maxPoints = uint64(len(documents))
	if limit > 0 && limit < uint64(len(documents)) {
		// Set specified points number limit
		maxPoints = limit
	}
	return documents, editors, maxPoints
}

func ProcessWikiPagesXml(reg *regexp.Regexp, tok xml.Token, currentText string, props map[string]string, layout string, field_tokenization string, debug int, documents []redisearch.Document, editors []string, docCount uint64, err error, dec *xml.Decoder) ([]redisearch.Document, []string, uint64, error ) {
	if debug > 1 {
		fmt.Fprintln(os.Stderr, "ProcessWikiPagesXml")
	}
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
			t, _ := time.Parse(layout, currentText)
			props[name] = fmt.Sprintf("%d", t.Unix())
		} else if name == "page" {

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

			if len(props["id"]) > 0 &&
				len(props["parentid"]) > 0 &&
				len(props["timestamp"]) > 0 &&
				len(props["username"]) > 0 {
				u1, _ := uuid.NewRandom()
				u2, _ := uuid.NewRandom()
				id := u1.String() + "-" + u2.String() + "-" + props["id"]
				doc := NewWikiPagesDocument(id, props)
				documents = append(documents, doc)
				editor_name := reg.ReplaceAllString(props["username"], "")
				editors = append(editors, editor_name)

				docCount++
				if debug > 0 {
					if docCount%1000 == 0 {
						fmt.Fprintln(os.Stderr, "At document "+strconv.Itoa(int(docCount)))
					}
				}
			}
			props = map[string]string{}
		}

		currentText = ""
	}
	tok, err = dec.RawToken()
	return documents, editors, docCount, err
}

func NewWikiPagesDocument(id string, props map[string]string) redisearch.Document {
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
	return doc
}
