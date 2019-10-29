package wiki

import (
	"encoding/xml"
	"fmt"
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_data/common"
	"github.com/RediSearch/redisearch-go/redisearch"
	"github.com/google/uuid"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// a Revision struct
type Sublink struct {
	XMLName  xml.Name `xml:"sublink"`
	Linktype string   `xml:"linktype"`
	Anchor   string   `xml:"anchor"`
	Link     string   `xml:"link"`
}

type Links struct {
	XMLName  xml.Name  `xml:"links"`
	Sublinks []Sublink `xml:"sublink"`
}

// the Page struct
type Doc struct {
	XMLName  xml.Name `xml:"doc"`
	Title    string   `xml:"title"`
	Url      string   `xml:"url"`
	Abstract string   `xml:"abstract"`
	Links    Links    `xml:"links"`
}

// our struct which contains the complete
// array of all Pages in the file
type Abstractwiki struct {
	XMLName xml.Name `xml:"feed"`
	Docs    []Doc    `xml:"doc"`
}

//<feed>
//<doc>
//<title>Wikipedia: Anarchism</title>
//<url>https://en.wikipedia.org/wiki/Anarchism</url>
//<abstract>Anarchism is an anti-authoritarian political philosophy that rejects hierarchies deemed unjust and advocates their replacement with self-managed, self-governed societies based on voluntary, cooperative institutions. These institutions are often described as stateless societies, although several authors have defined them more specifically as distinct institutions based on non-hierarchical or free associations.</abstract>
//<links>
//<sublink linktype="nav"><anchor>Etymology, terminology and definition</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Etymology,_terminology_and_definition</link></sublink>
//<sublink linktype="nav"><anchor>History</anchor><link>https://en.wikipedia.org/wiki/Anarchism#History</link></sublink>
//<sublink linktype="nav"><anchor>Prehistoric and ancient world</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Prehistoric_and_ancient_world</link></sublink>
//<sublink linktype="nav"><anchor>Modern era</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Modern_era</link></sublink>
//<sublink linktype="nav"><anchor>Post-World War II era</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Post-World_War_II_era</link></sublink>
//<sublink linktype="nav"><anchor>Anarchist schools of thought</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Anarchist_schools_of_thought</link></sublink>
//<sublink linktype="nav"><anchor>Classical</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Classical</link></sublink>
//<sublink linktype="nav"><anchor>Mutualism</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Mutualism</link></sublink>
//<sublink linktype="nav"><anchor>Collectivist anarchism</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Collectivist_anarchism</link></sublink>
//<sublink linktype="nav"><anchor>Anarcho-communism</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Anarcho-communism</link></sublink>
//<sublink linktype="nav"><anchor>Anarcho-syndicalism</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Anarcho-syndicalism</link></sublink>
//<sublink linktype="nav"><anchor>Individualist anarchism</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Individualist_anarchism</link></sublink>
//<sublink linktype="nav"><anchor>Post-classical and contemporary</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Post-classical_and_contemporary</link></sublink>
//<sublink linktype="nav"><anchor>Anarcha-feminism</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Anarcha-feminism</link></sublink>
//<sublink linktype="nav"><anchor>Anarcho-capitalism</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Anarcho-capitalism</link></sublink>
//<sublink linktype="nav"><anchor>Internal issues and debates</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Internal_issues_and_debates</link></sublink>
//<sublink linktype="nav"><anchor>Topics of interest</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Topics_of_interest</link></sublink>
//<sublink linktype="nav"><anchor>Anarchism and free love</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Anarchism_and_free_love</link></sublink>
//<sublink linktype="nav"><anchor>Anarchism and education</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Anarchism_and_education</link></sublink>
//<sublink linktype="nav"><anchor>Anarchism and the state</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Anarchism_and_the_state</link></sublink>
//<sublink linktype="nav"><anchor>Anarchism and violence</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Anarchism_and_violence</link></sublink>
//<sublink linktype="nav"><anchor>Anarchist strategies and tactics</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Anarchist_strategies_and_tactics</link></sublink>
//<sublink linktype="nav"><anchor>Criticism</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Criticism</link></sublink>
//<sublink linktype="nav"><anchor>Allegation of utopianism</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Allegation_of_utopianism</link></sublink>
//<sublink linktype="nav"><anchor>Industrial civilisation</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Industrial_civilisation</link></sublink>
//<sublink linktype="nav"><anchor>Tacit authoritarianism</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Tacit_authoritarianism</link></sublink>
//<sublink linktype="nav"><anchor>List of anarchist societies</anchor><link>https://en.wikipedia.org/wiki/Anarchism#List_of_anarchist_societies</link></sublink>
//<sublink linktype="nav"><anchor>See also</anchor><link>https://en.wikipedia.org/wiki/Anarchism#See_also</link></sublink>
//<sublink linktype="nav"><anchor>Foundational texts of anarchism</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Foundational_texts_of_anarchism</link></sublink>
//<sublink linktype="nav"><anchor>References</anchor><link>https://en.wikipedia.org/wiki/Anarchism#References</link></sublink>
//<sublink linktype="nav"><anchor>Citations</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Citations</link></sublink>
//<sublink linktype="nav"><anchor>Sources</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Sources</link></sublink>
//<sublink linktype="nav"><anchor>Further reading</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Further_reading</link></sublink>
//<sublink linktype="nav"><anchor>External links</anchor><link>https://en.wikipedia.org/wiki/Anarchism#External_links</link></sublink>
//</links>
//</doc>
//
//

// NewWikiAbrastractReader returns a new Core for the given input filename, seed, and maxQueries
func NewWikiAbrastractReader(filename string, stopwordsbl []string, seed int64, maxQueries int, debug int) *Core {
	documents := WikiAbstractParseXml(filename, uint64(maxQueries), debug, stopwordsbl, seed)
	oneWordQuery, twoWordQuery := generateQueriesFromDocument(seed, maxQueries, documents, stopwordsbl, debug)
	oneWordSpellCheckQuery, oneWordSpellCheckQueryDistance := generateSpellCheckQueriesFromDocument(seed, maxQueries, documents, stopwordsbl, debug)

	return NewCoreFromAbstract(oneWordQuery, twoWordQuery, oneWordSpellCheckQuery, oneWordSpellCheckQueryDistance)
}

func WikiAbstractParseXml(inputFilename string, limit uint64, debug int, stopwordsbl []string, seed int64) ([]redisearch.Document) {

	rand.Seed(seed)

	reg, err := regexp.Compile("[=,]*")
	if err != nil {
		log.Fatal(err)
	}

	var documents []redisearch.Document
	xmlFile, err := os.Open(inputFilename)
	if err != nil {
		log.Fatal(err)
	}
	defer xmlFile.Close()

	// read our opened xmlFile as a byte array.
	byteValue, err := ioutil.ReadAll(xmlFile)
	if err != nil {
		log.Fatal(err)
	}

	// we initialize our pages array
	var abstract Abstractwiki
	// we unmarshal our byteArray which contains our
	// xmlFiles content into 'pages' which we defined above
	err = xml.Unmarshal(byteValue, &abstract)
	if err != nil {
		log.Fatal(err)
	}

	if debug > 0 {
		fmt.Fprintln(os.Stderr, "abstract started reading "+inputFilename)
	}

	docCount := uint64(0)
	// we iterate through every user within our users array and
	// print out the user Type, their name, and their facebook url
	// as just an example
	for i := 0; i < len(abstract.Docs) && (uint64(i) < limit || limit == 0); i++ {
		d := abstract.Docs[i]

		d.Title = strings.TrimSpace(d.Title)
		d.Title = reg.ReplaceAllString(d.Title, "")
		d.Title = strings.Replace(d.Title, "Wikipedia:", "", 1)
		d.Title = redisearch.EscapeTextFileString(d.Title)

		d.Abstract = strings.TrimSpace(d.Abstract)
		d.Abstract = reg.ReplaceAllString(d.Abstract, "")
		d.Abstract = redisearch.EscapeTextFileString(d.Abstract)

		d.Url = strings.TrimSpace(d.Url)
		d.Url = reg.ReplaceAllString(d.Url, "")
		d.Url = redisearch.EscapeTextFileString(d.Url)

		u1, _ := uuid.NewRandom()
		docCount++
		id := fmt.Sprintf("%s-%d", u1.String(), docCount)
		doc := NewWikiAbstractDocument(id, d)
		documents = append(documents, doc)

		if debug > 0 {
			if docCount%1000 == 0 {
				fmt.Fprintln(os.Stderr, "At document "+strconv.Itoa(int(docCount)))
			}
		}

	}

	if debug > 0 {
		fmt.Fprintln(os.Stderr, fmt.Sprintf("finished reading %s. Total documents %d", inputFilename, len(documents)))
	}
	return documents
}

func generateQueriesFromDocument(seed int64, limit int, Docs []redisearch.Document, stopwordsbl []string, debug int) ([]string, [][]string) {
	var twoWordQuery [][]string
	var oneWordQuery []string
	var source []string

	rand.Seed(seed)
	// Make a Regex to say we only want letters and numbers
	reg, err := regexp.Compile("[^a-zA-Z0-9 ]+")
	if err != nil {
		log.Fatal(err)
	}
	nqueries := 0
	i := 0
	for (limit > nqueries || (limit == 0 && nqueries < len(Docs))) {
		d := Docs[i]
		//fmt.Fprintln(os.Stderr, "At document %v",d)
		used_field := rand.Intn(2)
		switch used_field {
		case 0:
			source = strings.Split(d.Properties["TITLE"].(string), " ")
		case 1:
			source = strings.Split(d.Properties["ABSTRACT"].(string), " ")
		}
		oneWordQuery = generateOneWordQuery(source, reg, stopwordsbl, oneWordQuery)
		nqueries = len(oneWordQuery)
		i++
		if i >= len(Docs) {
			i = 0
		}
		if debug > 0 {
			if nqueries%1000 == 0 {
				fmt.Fprintln(os.Stderr, "At queries "+strconv.Itoa(int(nqueries)))
			}
		}
	}
	nqueries = 0
	i = 0
	for (limit > nqueries || (limit == 0 && nqueries < len(Docs))) {

		d := Docs[i]

		used_field := rand.Intn(2)
		switch used_field {
		case 0:
			source = strings.Split(d.Properties["TITLE"].(string), " ")
		case 1:
			source = strings.Split(d.Properties["ABSTRACT"].(string), " ")
		}
		twoWordQuery = generateTwoWordQuery(source, reg, stopwordsbl, twoWordQuery)
		nqueries = len(twoWordQuery)
		i++
		if i >= len(Docs) {
			i = 0
		}
		if debug > 0 {
			if nqueries%1000 == 0 {
				fmt.Fprintln(os.Stderr, "At queries "+strconv.Itoa(int(nqueries)))
			}
		}
	}
	return oneWordQuery, twoWordQuery

}

func generateSpellCheckQueriesFromDocument(seed int64, limit int, Docs []redisearch.Document, stopwordsbl []string, debug int) ([]string, []int) {
	var oneWordSpellCheckQuery []string
	var oneWordSpellCheckQueryDistance []int
	var source []string

	rand.Seed(seed)
	// Make a Regex to say we only want letters and numbers
	reg, err := regexp.Compile("[^a-zA-Z0-9 ]+")
	if err != nil {
		log.Fatal(err)
	}
	nqueries := 0
	i := 0
	for (limit > nqueries || (limit == 0 && nqueries < len(Docs))) {
		d := Docs[i]
		//fmt.Fprintln(os.Stderr, "At document %v",d)
		used_field := rand.Intn(2)
		switch used_field {
		case 0:
			source = strings.Split(d.Properties["TITLE"].(string), " ")
		case 1:
			source = strings.Split(d.Properties["ABSTRACT"].(string), " ")
		}

		oneWordSpellCheckQuery, oneWordSpellCheckQueryDistance = generateOneWordSpellCheckQuery(source, reg, stopwordsbl, oneWordSpellCheckQuery, oneWordSpellCheckQueryDistance)
		nqueries = len(oneWordSpellCheckQuery)
		i++
		if i >= len(Docs) {
			i = 0
		}
		if debug > 0 {
			if nqueries%1000 == 0 {
				fmt.Fprintln(os.Stderr, "At queries "+strconv.Itoa(int(nqueries)))
			}
		}
	}
	return oneWordSpellCheckQuery, oneWordSpellCheckQueryDistance

}

func generateOneWordSpellCheckQuery(source []string, reg *regexp.Regexp, stopwordsbl []string, oneWordQuery []string, oneWordSpellCheckQueryDistance []int) ([]string, []int) {
	first_word := ""
	if len(source)-1 >= 1 {
		suffixPrefixDiff := false
		// try out 10 times prior to passing up to next two word combination
		for stemRetry := 0; stemRetry < 10 && suffixPrefixDiff == false; stemRetry++ {

			first_word_pos := rand.Intn(len(source) - 1)
			first_word = strings.TrimSpace(source[first_word_pos])
			first_word = reg.ReplaceAllString(first_word, "")

			if len(first_word) > 0 {

				containsStopWord := false
				i := sort.SearchStrings(stopwordsbl, strings.ToLower(first_word))
				if i < len(stopwordsbl) && stopwordsbl[i] == strings.ToLower(first_word) {
					containsStopWord = true
				}

				// avoid having stopwords on the query
				if containsStopWord == false {
					suffixPrefixDiff = true
				}
			}
		}

		if len(first_word) > 0 && suffixPrefixDiff == true {

			var newWord = first_word
			maxChanges := math.Min(float64(len(first_word)-2), 4)
			numberChanges := 1
			effectiveChanges := 0
			if maxChanges > 0 {
				numberChanges = rand.Intn(int(maxChanges))
				// the word needs to have at least 4 chars
				for atChange := 0; atChange < numberChanges; atChange++ {
					if len(newWord) > 3 {
						charPos := rand.Intn(len(newWord)-2) + 1
						// non-negative pseudo-random number in [0,4)
						// 0 - delete char word[:charPos] + word[:charPos+1:]
						// 1 - insert random char
						// 2 - replace with random char
						// 3 - switch adjacent chars
						switch rand.Intn(4) {
						case 0:
							newWord = newWord[:charPos] + newWord[charPos+1:]
						case 1:
							newWord = newWord[:charPos] + string(letters[rand.Intn(len(letters))]) + newWord[charPos+1:]
						case 2:
							newWord = newWord[:charPos] + string(letters[rand.Intn(len(letters))]) + newWord[charPos+1:]
						case 3:
							adjacentPos := charPos + 1
							newWord = newWord[:charPos] + newWord[adjacentPos:adjacentPos] + newWord[charPos:charPos] + newWord[adjacentPos+1:]
						}
						effectiveChanges = effectiveChanges + 1
					}
				}

			}
			containsStopWord := false
			i := sort.SearchStrings(stopwordsbl, strings.ToLower(newWord))
			if i < len(stopwordsbl) && stopwordsbl[i] == strings.ToLower(newWord) {
				containsStopWord = true
			}
			if containsStopWord == false {
				//if debug > 0 {
				fmt.Fprintln(os.Stderr, "docs read %s ", newWord)
				//}
				oneWordQuery = append(oneWordQuery, newWord)
				oneWordSpellCheckQueryDistance = append(oneWordSpellCheckQueryDistance, effectiveChanges+1)
			}
		}
	}
	return oneWordQuery, oneWordSpellCheckQueryDistance
}

func generateOneWordQuery(source []string, reg *regexp.Regexp, stopwordsbl []string, oneWordQuery []string) ( []string) {
	first_word := ""
	if len(source)-1 >= 1 {
		suffixPrefixDiff := false
		// try out 10 times prior to passing up to next two word combination
		for stemRetry := 0; stemRetry < 10 && suffixPrefixDiff == false; stemRetry++ {

			first_word_pos := rand.Intn(len(source) - 1)
			first_word = strings.TrimSpace(source[first_word_pos])
			first_word = reg.ReplaceAllString(first_word, "")

			if len(first_word) > 0 {

				containsStopWord := false
				i := sort.SearchStrings(stopwordsbl, strings.ToLower(first_word))
				if i < len(stopwordsbl) && stopwordsbl[i] == strings.ToLower(first_word) {
					containsStopWord = true
				}

				// avoid having stopwords on the query
				if containsStopWord == false {
					suffixPrefixDiff = true
				}
			}
		}

		if len(first_word) > 0 && suffixPrefixDiff == true {
			oneWordQuery = append(oneWordQuery, first_word)
		}
	}
	return oneWordQuery
}

func generateTwoWordQuery(source []string, reg *regexp.Regexp, stopwordsbl []string, TwoWordQuery [][]string) ([][]string) {
	first_word := ""
	second_word := ""
	if len(source)-1 >= 1 {
		suffixPrefixDiff := false
		// try out 10 times prior to passing up to next two word combination
		for stemRetry := 0; stemRetry < 10 && suffixPrefixDiff == false; stemRetry++ {

			second_word_pos := rand.Intn(len(source)-1) + 1
			second_word = strings.TrimSpace(source[second_word_pos])
			second_word = reg.ReplaceAllString(second_word, "")

			first_word_pos := rand.Intn(second_word_pos)
			first_word = strings.TrimSpace(source[first_word_pos])
			first_word = reg.ReplaceAllString(first_word, "")

			if len(first_word) > 0 && len(second_word) > 0 {

				containsStopWord := false
				i := sort.SearchStrings(stopwordsbl, strings.ToLower(first_word))
				j := sort.SearchStrings(stopwordsbl, strings.ToLower(second_word))
				if i < len(stopwordsbl) && stopwordsbl[i] == strings.ToLower(first_word) {
					containsStopWord = true
				}

				if j < len(stopwordsbl) && stopwordsbl[j] == strings.ToLower(second_word) {
					containsStopWord = true
				}
				// avoid having stopwords on the query
				// avoid having two equal words to be used on the same 2 word combination
				// avoid words with equal sufixes and prefixes ( prevent two equal words after stemming )
				if containsStopWord == false && first_word != second_word && first_word[0] != second_word[0] && first_word[len(first_word)-1] != second_word[len(second_word)-1] {
					suffixPrefixDiff = true
				}
			}
		}

		if len(first_word) > 0 && len(second_word) > 0 && suffixPrefixDiff == true {
			wordsSlice := make([]string, 2, 2)
			wordsSlice[0] = first_word
			wordsSlice[1] = second_word
			TwoWordQuery = append(TwoWordQuery, wordsSlice)
		}
	}
	return TwoWordQuery
}

// WikiAbstractSimulatorConfig is used to create a FTSSimulator.
type WikiAbstractSimulatorConfig commonFTSSimulatorConfig

// NewSimulator produces a Simulator that conforms to the given SimulatorConfig over the specified interval
func (c *WikiAbstractSimulatorConfig) NewSimulator(limit uint64, inputFilename string, debug int, stopwords []string, seed int64) common.Simulator {
	documents := WikiAbstractParseXml(inputFilename, limit, debug, stopwords, seed)

	if debug > 0 {
		fmt.Fprintln(os.Stderr, "docs read %d ", uint64(len(documents)))
	}
	sim := &FTSSimulator{&commonFTSSimulator{
		madeDocuments: 0,
		maxDocuments:  uint64(len(documents)),

		recordIndex: 0,
		records:     documents,
	}}

	return sim
}

func NewWikiAbstractDocument(id string, d Doc) redisearch.Document {
	doc := redisearch.NewDocument(id, 1.0).
		Set("TITLE", d.Title).
		Set("URL", d.Url).
		Set("ABSTRACT", d.Abstract)

	return doc
}
