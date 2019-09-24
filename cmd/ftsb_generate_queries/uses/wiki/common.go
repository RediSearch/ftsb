package wiki

import (
	"encoding/xml"
	"fmt"
	"github.com/RediSearch/ftsb/cmd/ftsb_generate_queries/utils"
	"github.com/RediSearch/ftsb/query"
	"io"
	"log"
	"math/rand"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
)

const (
	//////////////////////////
	// Full text search queries
	//////////////////////////
	// LabelSimple1WordQuery is the label prefix for queries of the Simple 1 Word Query
	LabelSimple1WordQuery = "simple-1word-query"
	// LabelTwoWordIntersectionQuery is the label prefix for queries of the Simple 2 Word Intersection Query
	LabelTwoWordIntersectionQuery = "2word-intersection-query"
	// LabelTwoWordIntersectionQuery is the label prefix for queries of the Simple 2 Word Union Query
	LabelSimple2WordUnionQuery = "2word-union-query"
	// LabelExact3WordMatch is the label for the lastpoint query
	LabelExact3WordMatch = "exact-3word-match"
	// LabelAutocomplete1100Top3 is the label prefix for queries of the max all variety
	LabelAutocomplete1100Top3 = "autocomplete-1100-top3"
	// LabelSimple1WordQuery is the label prefix for queries of the Simple 1 Word Query
	LabelSimple2WordBarackObama = "simple-2word-barack-obama"

	//////////////////////////
	// Spell Check queries
	//////////////////////////

	// LabelSimple1WordQuery is the label prefix for queries of the Simple 1 Word Spell Check
	LabelSimple1WordSpellCheck = "simple-1word-spellcheck"

	//////////////////////////
	// Autocomplete queries
	//////////////////////////

	//////////////////////////
	// Synonym queries
	//////////////////////////



)

// for ease of testing
var fatal = log.Fatalf

// Core is the common component of all generators for all systems
type Core struct {
	TwoWordIntersectionQueryIndexPosition uint64
	TwoWordIntersectionQueryIndex         uint64
	TwoWordIntersectionQueries            []string
	TwoWordUnionQueryIndexPosition        uint64
	TwoWordUnionQueryIndex                uint64
	TwoWordUnionQueries                   []string
	OneWordQueryIndexPosition             uint64
	OneWordQueryIndex                     uint64
	OneWordQueries                        []string
}

// NewWikiAbrastractReader returns a new Core for the given input filename, seed, and maxQueries
func NewWikiAbrastractReader(filename string, stopwordsbl []string, seed int64, maxQueries int) *Core {
	//https://github.com/RediSearch/RediSearch/issues/307
	//prevent field tokenization ,.<>{}[]"':;!@#$%^&*()-+=~
	//field_tokenization := ",.<>{}[]\"':;!@#$%^&*()-+=~"

	// Make a Regex to say we only want letters and numbers
	reg, err := regexp.Compile("[^a-zA-Z0-9]+")
	if err != nil {
		log.Fatal(err)
	}

	rand.Seed(seed)
	var twoWordIntersectionQuery []string
	var twoWordUnionQuery []string
	var oneWordQuery []string
	if filename == "" {
		fmt.Println("No input file provided. skipping input reading ")
	} else {
		fmt.Println("Reading " + filename)
		xmlFile, err := os.Open(filename)
		if err != nil {
			log.Fatal("Error while opening input file ", err)
		}
		dec := xml.NewDecoder(xmlFile)

		tok, err := dec.RawToken()

		props := map[string]string{}
		var currentText string
		queryCount := 0
		oneWordQueryCount := 0
		twoWordUnionQueryCount := 0

		for err != io.EOF && maxQueries > queryCount {
			used_field := rand.Intn(2)

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
					props["title"] = strings.TrimPrefix(strings.TrimSpace(props["title"]), "Wikipedia: ")
					props["abstract"] = strings.TrimSpace(props["abstract"])
					props["url"] = strings.TrimSpace(props["url"])
					props["title"] = strings.ReplaceAll(props["title"], "\"", "\\\"")
					props["abstract"] = strings.ReplaceAll(props["abstract"], "\"", "\\\"")
					props["url"] = strings.ReplaceAll(props["url"], "\"", "\\\"")
					props["title"] = strings.TrimSpace(props["title"])
					props["abstract"] = strings.TrimSpace(props["abstract"])
					props["url"] = strings.TrimSpace(props["url"])
					var source []string
					first_word := ""
					second_word := ""
					switch used_field {
					case 0:
						source = strings.Split(props["title"], " ")
					case 1:
						source = strings.Split(props["abstract"], " ")
					}
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

						if len(first_word) > 0 {
							oneWordQuery = append(oneWordQuery, first_word)
							oneWordQueryCount++
						}

						if len(second_word) > 0 {
							oneWordQuery = append(oneWordQuery, second_word)
							oneWordQueryCount++
						}

						if len(first_word) > 0 && len(second_word) > 0 && suffixPrefixDiff == true {
							twoWordIntersectionQuery = append(twoWordIntersectionQuery, first_word+" "+second_word)
							queryCount++
						}

						if len(first_word) > 0 && len(second_word) > 0 && suffixPrefixDiff == true {
							twoWordUnionQuery = append(twoWordUnionQuery, first_word+"|"+second_word)
							twoWordUnionQueryCount++
						}
					}
					props = map[string]string{}
				}
				currentText = ""
			}

			tok, err = dec.RawToken()
		}
	}
	return &Core{
		0,
		uint64(len(twoWordIntersectionQuery)),
		twoWordIntersectionQuery,
		0,
		uint64(len(twoWordUnionQuery)),
		twoWordUnionQuery,
		0,
		uint64(len(oneWordQuery)),
		oneWordQuery,
	}
}

// TwoWordIntersectionQueryFiller is a type that can fill in a single query
type TwoWordIntersectionQueryFiller interface {
	TwoWordIntersectionQuery(query.Query)
}

// TwoWordUnionQueryFiller is a type that can fill in a single query
type TwoWordUnionQueryFiller interface {
	TwoWordUnionQuery(query.Query)
}

// OneWordQueryFiller is a type that can fill in a single query
type Simple1WordQueryFiller interface {
	Simple1WordQuery(query.Query)
}

// SimpleTwoWordBarackObamaQueryFiller is a type that can fill in a single  query
type Simple2WordBarackObamaFiller interface {
	Simple2WordBarackObama(query.Query)
}


// OneWordQueryFiller is a type that can fill in a single query
type Simple1WordSpellCheckQueryFiller interface {
	Simple1WordSpellCheck(query.Query)
}

func panicUnimplementedQuery(dg utils.EnWikiAbstractGenerator) {
	panic(fmt.Sprintf("database (%v) does not implement query", reflect.TypeOf(dg)))
}
