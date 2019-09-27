package wiki

import (
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"regexp"
	"sort"
	"strings"
)

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

				//#<page>
				//#    <title>Stockton Airport</title>
				//#    <ns>0</ns>
				//#    <id>7697612</id>
				//#    <revision>
				//#      <id>865514439</id>
				//#      <parentid>479135040</parentid>
				//#      <timestamp>2018-10-24T11:44:29Z</timestamp>
				//#      <contributor>
				//#        <username>Narky Blert</username>
				//#        <id>22041646</id>
				//#      </contributor>
				//#      <minor />
				//#      <comment>ce</comment>
				//#      <model>wikitext</model>
				//#      <sha1>qxcai6tfmnb22471c9xe3qamuejvst9</sha1>
				//#    </revision>
				//#  </page>
				//
				if (name == "title" ||
					name == "ns" ||
					name == "id" ||
					name == "parentid" ||
					name == "timestamp" ||
					name == "username" ||
					name == "comment") {
					props[name] = currentText
				} else if name == "page" {
					props["title"] = strings.TrimSpace(props["title"])
					props["title"] = strings.ReplaceAll(props["title"], "\"", "\\\"")
					props["ns"] = strings.TrimSpace(props["ns"])
					props["id"] = strings.TrimSpace(props["id"])

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
