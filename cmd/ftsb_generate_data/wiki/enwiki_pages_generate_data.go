package wiki

import (
	"encoding/xml"
	"fmt"
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_data/common"
	"github.com/RediSearch/redisearch-go/redisearch"
	"github.com/google/uuid"
	"log"
	"math"
	"regexp"

	//"io"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

// WikiAbstractSimulatorConfig is used to create a FTSSimulator.
type WikiPagesSimulatorConfig common.CommonFTSSimulatorConfig

// NewSimulator produces a Simulator that conforms to the given SimulatorConfig over the specified interval
func (c *WikiPagesSimulatorConfig) NewSimulator(limit uint64, inputFilename string, debug int, stopwords []string, seed int64) common.Simulator {
	if debug > 0 {
		fmt.Fprintln(os.Stderr, fmt.Sprintf("Using random seed %d", seed))
		fmt.Fprintln(os.Stderr, fmt.Sprintf("stopwords being excluded from generation %s", stopwords))
	}
	documents, _, maxPoints, _, _ := WikiPagesParseXml(inputFilename, limit, debug, stopwords, seed)
	if debug > 0 {
		fmt.Fprintln(os.Stderr, fmt.Sprintf("pages read %d ", maxPoints))
	}
	sim := &common.FTSSimulator{&common.CommonFTSSimulator{
		MadeDocuments: 0,
		MaxDocuments:  maxPoints,

		RecordIndex: 0,
		Records:     documents,
	}}
	if debug > 0 {
		fmt.Fprintln(os.Stderr, fmt.Sprintf("docs generated %d ", uint64(len(documents))))
	}
	return sim
}

// NewSimulator produces a Simulator that conforms to the given SimulatorConfig over the specified interval
func (c *WikiPagesSimulatorConfig) NewSyntheticsSimulator(limit uint64, debug int, stopwords []string, numberFields, syntheticsFieldDataSize, maxCardinalityPerField uint64, seed int64) common.Simulator {
	if debug > 0 {
		fmt.Fprintln(os.Stderr, fmt.Sprintf("Using random seed %d", seed))
		fmt.Fprintln(os.Stderr, fmt.Sprintf("stopwords being excluded from generation %s", stopwords))
		fmt.Fprintln(os.Stderr, fmt.Sprintf("Preparing to simulate %d docs, with %d fields, field size of %d, and max cardinality per field of %d", limit, numberFields, syntheticsFieldDataSize, maxCardinalityPerField))
	}
	rand.Seed(seed)
	var documents []redisearch.Document
	sim := &common.FTSSimulator{&common.CommonFTSSimulator{
		MadeDocuments: 0,
		MaxDocuments:  0,

		RecordIndex: 0,
		Records:     documents,
	}}
	if debug > 0 {
		fmt.Fprintln(os.Stderr, fmt.Sprintf("docs generated %d ", uint64(len(documents))))
	}
	return sim
}

// our struct which contains the complete
// array of all Pages in the file
type Mediawiki struct {
	XMLName xml.Name `xml:"mediawiki"`
	Pages   []Page   `xml:"page"`
}

// the Page struct
type Page struct {
	XMLName  xml.Name `xml:"page"`
	Title    string   `xml:"title"`
	Ns       int      `xml:"ns"`
	Id       string   `xml:"id"`
	Revision Revision `xml:"revision"`
}

// a Revision struct
type Revision struct {
	XMLName     xml.Name    `xml:"revision"`
	Id          int         `xml:"id"`
	ParentId    int         `xml:"parentid"`
	Timestamp   string      `xml:"timestamp"`
	Contributor Contributor `xml:"contributor"`
	Comment     string      `xml:"comment"`
}

// a simple struct which contains all our
// Contributor info
type Contributor struct {
	XMLName  xml.Name `xml:"contributor"`
	Username string   `xml:"username"`
	Id       int      `xml:"id"`
}

// NewWikiAbrastractReader returns a new Core for the given input filename, seed, and maxQueries
func NewWikiPagesReader(filename string, stopwordsbl []string, seed int64, maxQueries int, debug int) *Core {
	_, editors, _, inferiorLimit, superiorLimit := WikiPagesParseXml(filename, uint64(maxQueries), debug, stopwordsbl, seed)
	return NewCore(editors, seed, inferiorLimit, superiorLimit)
}

func WikiPagesParseXml(inputFilename string, limit uint64, debug int, stopwordsbl []string, seed int64) ([]redisearch.Document, []string, uint64, int64, int64) {
	//https://github.com/RediSearch/RediSearch/issues/307
	//prevent field tokenization ,.<>{}[]"':;!@#$%^&*()-+=~
	//field_tokenization := ",.<>{}[]\"':;!@#$%^&*()-+=~"
	// Make a Regex to say we only want common.Letters and numbers
	reg, err := regexp.Compile("[^a-zA-Z0-9 ]+")
	if err != nil {
		log.Fatal(err)
	}

	rand.Seed(seed)

	var editors []string
	var documents []redisearch.Document
	xmlFile, _ := os.Open(inputFilename)
	defer xmlFile.Close()

	// read our opened xmlFile as a byte array.
	byteValue, _ := ioutil.ReadAll(xmlFile)

	// we initialize our pages array
	var pages Mediawiki
	// we unmarshal our byteArray which contains our
	// xmlFiles content into 'pages' which we defined above
	xml.Unmarshal(byteValue, &pages)

	//dec := xml.NewDecoder(xmlFile)
	maxPoints := limit
	//
	layout := "2006-01-02T15:04:05Z"
	var inferiorLimit int64 = math.MaxInt64
	var superiorLimit int64 = math.MinInt64
	len_pages := len(pages.Pages)

	if debug > 0 {
		fmt.Fprintln(os.Stderr, fmt.Sprintf("Pages started reading %s.Total distinct documents: %d. Want to generate %d.", inputFilename, len_pages, limit))
		if limit > uint64(len_pages) {
			fmt.Fprintln(os.Stderr, "\tWill round robin on input file since limit is larger than dataset .")
		}
	}
	docCount := uint64(0)
	// we iterate through every user within our users array and
	// print out the user Type, their name, and their facebook url
	// as just an example
	for i := 0;
	// if there is a limit and it's lower than the total number of docs
	// or the limit is 0 and we just iterate over the entire result-set once
		(i < len_pages && (uint64(i) < limit || limit == 0)) ||
			// if the limit is higher than the total number of docs
			(uint64(i) < limit && limit > uint64(len_pages)) && limit != 0;
	i++ {
		page := pages.Pages[i%len_pages]

		page.Revision.Timestamp = strings.TrimSpace(page.Revision.Timestamp)
		t, _ := time.Parse(layout, page.Revision.Timestamp)
		ts := t.Unix()
		if inferiorLimit > ts {
			inferiorLimit = ts
		}
		if superiorLimit < ts {
			superiorLimit = ts
		}
		page.Revision.Timestamp = fmt.Sprintf("%d", ts)
		page.Title = reg.ReplaceAllString(page.Title, "")
		page.Title = strings.TrimSpace(page.Title)
		page.Revision.Comment = reg.ReplaceAllString(page.Revision.Comment, "")
		page.Revision.Comment = strings.TrimSpace(page.Revision.Comment)
		page.Revision.Contributor.Username = reg.ReplaceAllString(page.Revision.Contributor.Username, "")
		page.Revision.Contributor.Username = strings.TrimSpace(page.Revision.Contributor.Username)

		u1, _ := uuid.NewRandom()
		docCount++
		id := fmt.Sprintf("%s-%d", u1.String(), docCount)
		doc := NewWikiPagesDocument(id, page)
		documents = append(documents, doc)
		if len(page.Revision.Contributor.Username) > 2 {
			editors = append(editors, page.Revision.Contributor.Username)
		}

		if debug > 0 {
			if docCount%1000 == 0 {
				fmt.Fprintln(os.Stderr, "At document "+strconv.Itoa(int(docCount)))
			}
		}

	}

	maxPoints = uint64(len(documents))
	if limit > 0 && limit < uint64(len(documents)) {
		// Set specified points number limit
		maxPoints = limit
	}
	if debug > 0 {
		fmt.Fprintln(os.Stderr, fmt.Sprintf("finished reading %s. Total documents %d Time interval [ %d , %d ]", inputFilename, maxPoints, inferiorLimit, superiorLimit))
	}
	return documents, editors, maxPoints, inferiorLimit, superiorLimit
}

func NewWikiPagesDocument(id string, page Page) redisearch.Document {
	doc := redisearch.NewDocument(id, 1.0).
		Set("TITLE", page.Title).
		Set("NAMESPACE", page.Ns).
		Set("ID", page.Id).
		Set("PARENT_REVISION_ID", page.Revision.ParentId).
		Set("CURRENT_REVISION_TIMESTAMP", page.Revision.Timestamp).
		Set("CURRENT_REVISION_ID", page.Revision.Id).
		Set("CURRENT_REVISION_EDITOR_USERNAME", page.Revision.Contributor.Username).
		Set("CURRENT_REVISION_EDITOR_IP", fmt.Sprintf("%d.%d.%d.%d", rand.Intn(256), rand.Intn(256), rand.Intn(256), rand.Intn(256))).
		Set("CURRENT_REVISION_EDITOR_USERID", page.Revision.Contributor.Id).
		Set("CURRENT_REVISION_EDITOR_COMMENT", page.Revision.Comment).
		Set("CURRENT_REVISION_CONTENT_LENGTH", len(page.Revision.Comment))
	return doc
}
