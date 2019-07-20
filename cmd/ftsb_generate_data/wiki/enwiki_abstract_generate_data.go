package wiki

import (
	"encoding/xml"
	"github.com/filipecosta90/ftsb/cmd/ftsb_generate_data/common"
	"github.com/filipecosta90/ftsb/cmd/ftsb_generate_data/serialize"
	"github.com/google/uuid"
	"io"
	"os"
	"path"
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

// Next advances a Document to the next state in the generator.
func (d *FTSSimulator) CreateIdx(Idx string, w io.Writer) {
	var buf []byte

	// FT.CREATE {index}
	//    [MAXTEXTFIELDS] [TEMPORARY {seconds}] [NOOFFSETS] [NOHL] [NOFIELDS] [NOFREQS]
	//    [STOPWORDS {num} {stopword} ...]
	//    SCHEMA {field} [TEXT [NOSTEM] [WEIGHT {weight}] [PHONETIC {matcher}] | NUMERIC | GEO | TAG [SEPARATOR {sep}] ] [SORTABLE][NOINDEX] ...
	buf = append(buf, []byte("FT.CREATE "+Idx+" SCHEMA TITLE TEXT WEIGHT 5 URL TEXT WEIGHT 5 ABSTRACT TEXT WEIGHT 1")...)
	buf = append(buf, []byte("\n")...)
	_, _ = w.Write(buf)

}

func (s *FTSSimulator) populateDocument(p *serialize.Document) bool {
	record := &s.records[s.recordIndex]

	p.Id = record.Id
	p.Title = record.Title
	p.Url = record.Url
	p.Abstract = record.Abstract

	ret := s.recordIndex < uint64(len(s.records))
	s.recordIndex = s.recordIndex + 1
	s.madePoints = s.madePoints + 1
	return ret
}

// FTSSimulatorConfig is used to create a FTSSimulator.
type FTSSimulatorConfig commonFTSSimulatorConfig

// NewSimulator produces a Simulator that conforms to the given SimulatorConfig over the specified interval
func (c *FTSSimulatorConfig) NewSimulator(limit uint64, inputFilename string, IdxName string) common.Simulator {
	var documents []serialize.Document
	xmlFile, _ := os.Open(inputFilename)
	dec := xml.NewDecoder(xmlFile)

	maxPoints := limit
	tok, err := dec.RawToken()

	props := map[string]string{}
	var currentText string
	for err != io.EOF {

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
				id := IdxName + "-" + u2.String() + "-" + path.Base(props["url"])
				props["title"] = strings.TrimPrefix(strings.TrimSpace(props["title"]), "Wikipedia: ")
				props["abstract"] = strings.TrimSpace(props["abstract"])
				props["url"] = strings.TrimSpace(props["url"])
				props["title"] = strings.ReplaceAll(props["title"], "\"", "\\\"")
				props["abstract"] = strings.ReplaceAll(props["abstract"], "\"", "\\\"")
				props["url"] = strings.ReplaceAll(props["url"], "\"", "\\\"")
				documents = append(documents, serialize.Document{[]byte(id), []byte( "\"" + props["title"] + "\"" ), []byte( "\"" + props["url"] + "\""), []byte( "\"" + props["abstract"] + "\"" )})
				props = map[string]string{}
			}
			currentText = ""
		}

		tok, err = dec.RawToken()

	}

	maxPoints = uint64(len(documents))
	if limit > 0 && limit < uint64(len(documents)) {
		// Set specified points number limit
		maxPoints = limit
	}
	sim := &FTSSimulator{&commonFTSSimulator{
		madePoints: 0,
		maxPoints:  maxPoints,

		recordIndex: 0,
		records:     documents,
	}}

	return sim
}
