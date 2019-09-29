package wiki

import (
	"encoding/xml"
	"fmt"
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_data/common"
	"github.com/RediSearch/redisearch-go/redisearch"
	"github.com/google/uuid"
	"log"
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
type WikiPagesSimulatorConfig commonFTSSimulatorConfig

// NewSimulator produces a Simulator that conforms to the given SimulatorConfig over the specified interval
func (c *WikiPagesSimulatorConfig) NewSimulator(limit uint64, inputFilename string, debug int, stopwords []string, seed int64) common.Simulator {
	documents, _, maxPoints := WikiPagesParseXml(inputFilename, limit, debug, stopwords, seed)
	if debug > 0 {
		fmt.Fprintln(os.Stderr, "pages read %d ", maxPoints)
	}
	sim := &FTSSimulator{&commonFTSSimulator{
		madeDocuments: 0,
		maxDocuments:  maxPoints,

		recordIndex: 0,
		records:     documents,
	}}

	return sim
}

// our struct which contains the complete
// array of all Users in the file
type Mediawiki struct {
	XMLName xml.Name `xml:"mediawiki"`
	Pages   []Page   `xml:"page"`
}

// the user struct, this contains our
// Type attribute, our user's name and
// a social struct which will contain all
// our social links
type Page struct {
	XMLName  xml.Name `xml:"page"`
	Title    string   `xml:"title"`
	Ns       int      `xml:"ns"`
	Id       string   `xml:"id"`
	Revision Revision `xml:"revision"`
}

// a simple struct which contains all our
// social links
type Revision struct {
	XMLName     xml.Name    `xml:"revision"`
	Id          int         `xml:"id"`
	ParentId    int         `xml:"parentid"`
	Timestamp   string      `xml:"timestamp"`
	Contributor Contributor `xml:"contributor"`
	Comment     string      `xml:"comment"`
}

// a simple struct which contains all our
// social links
type Contributor struct {
	XMLName  xml.Name `xml:"contributor"`
	Username string   `xml:"username"`
	Id       int      `xml:"id"`
}

// NewWikiAbrastractReader returns a new Core for the given input filename, seed, and maxQueries
func NewWikiPagesReader(filename string, stopwordsbl []string, seed int64, maxQueries int, debug int) *Core {
	_, editors, _ := WikiPagesParseXml(filename, uint64(maxQueries), debug, stopwordsbl, seed)
	return NewCore(editors)
}

func WikiPagesParseXml(inputFilename string, limit uint64, debug int, stopwordsbl []string, seed int64) ([]redisearch.Document, []string, uint64) {
	//https://github.com/RediSearch/RediSearch/issues/307
	//prevent field tokenization ,.<>{}[]"':;!@#$%^&*()-+=~
	//field_tokenization := ",.<>{}[]\"':;!@#$%^&*()-+=~"
	// Make a Regex to say we only want letters and numbers
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

	// we initialize our Users array
	var pages Mediawiki
	// we unmarshal our byteArray which contains our
	// xmlFiles content into 'users' which we defined above
	xml.Unmarshal(byteValue, &pages)

	//dec := xml.NewDecoder(xmlFile)
	maxPoints := limit
	//
	layout := "2006-01-02T15:04:05Z"
	//props := map[string]string{}
	//var currentText string
	if debug > 0 {
		fmt.Fprintln(os.Stderr, "pages started reading "+inputFilename)
	}
	docCount := uint64(0)
	// we iterate through every user within our users array and
	// print out the user Type, their name, and their facebook url
	// as just an example
	for i := 0; i < len(pages.Pages) && (uint64(i) < limit || limit == 0); i++ {
		page := pages.Pages[i]

		page.Revision.Timestamp = strings.TrimSpace(page.Revision.Timestamp)
		t, _ := time.Parse(layout, page.Revision.Timestamp)
		page.Revision.Timestamp = fmt.Sprintf("%d", t.Unix())
		page.Title = reg.ReplaceAllString(page.Title, "")
		page.Revision.Comment = reg.ReplaceAllString(page.Revision.Comment, "")
		page.Revision.Contributor.Username = reg.ReplaceAllString(page.Revision.Contributor.Username, "")

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
