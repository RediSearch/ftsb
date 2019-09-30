package serialize

// WikiAbstract wraps a single document. It stores database-agnostic data
import (
	"io"
)

// WikiAbstract wraps a single document. It stores database-agnostic data
// representing one WikiAbstract
//
// Internally, WikiAbstract uses byte slices instead of strings to try to minimize
// overhead.
type RediSearchDocument struct {
	Id, Title, Namespace                                                                []byte
	ParentRevisionId, CurrentRevisionTS, CurrentRevisionId                              []byte
	CurrentRevisionEditorUsername, CurrentRevisionEditorIP, CurrentRevisionEditorUserId []byte
	CurrentRevisionEditorComment, CurrentRevisionEditorContentLength                    []byte
}

// NewWikiAbstract returns a new empty WikiAbstract
func NewWikiPages() *RediSearchDocument {
	return &RediSearchDocument{
		Id:                                 make([]byte, 0),
		Title:                              make([]byte, 0),
		Namespace:                          make([]byte, 0),
		ParentRevisionId:                   make([]byte, 0),
		CurrentRevisionTS:                  make([]byte, 0),
		CurrentRevisionId:                  make([]byte, 0),
		CurrentRevisionEditorUsername:      make([]byte, 0),
		CurrentRevisionEditorIP:            make([]byte, 0),
		CurrentRevisionEditorUserId:        make([]byte, 0),
		CurrentRevisionEditorComment:       make([]byte, 0),
		CurrentRevisionEditorContentLength: make([]byte, 0),
	}
}

// Reset clears all information from this WikiAbstract so it can be reused.
func (p *RediSearchDocument) Reset() {
	p.Id = p.Id[:0]
	p.Title = p.Title[:0]
	p.Namespace = p.Namespace[:0]
	p.ParentRevisionId = p.ParentRevisionId[:0]
	p.CurrentRevisionTS = p.CurrentRevisionTS[:0]
	p.CurrentRevisionId = p.CurrentRevisionId[:0]
	p.CurrentRevisionEditorUsername = p.CurrentRevisionEditorUsername[:0]
	p.CurrentRevisionEditorIP = p.CurrentRevisionEditorIP[:0]
	p.CurrentRevisionEditorUserId = p.CurrentRevisionEditorUserId[:0]
	p.CurrentRevisionEditorComment = p.CurrentRevisionEditorComment[:0]
	p.CurrentRevisionEditorContentLength = p.CurrentRevisionEditorContentLength[:0]
}

// DocumentSerializer serializes a WikiAbstract for writing
type WikiPagesSerializer interface {
	Serialize(p *RediSearchDocument, w io.Writer) error
}

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
