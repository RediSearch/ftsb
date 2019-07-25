package serialize

// Document wraps a single document. It stores database-agnostic data
import (
	"io"
)

// Document wraps a single document. It stores database-agnostic data
// representing one Document
//
// Internally, Document uses byte slices instead of strings to try to minimize
// overhead.
type Document struct {
	Id, Title, Url, Abstract []byte
}

// NewDocument returns a new empty Document
func NewDocument() *Document {
	return &Document{
		Id:       make([]byte, 0),
		Title:    make([]byte, 0),
		Url:      make([]byte, 0),
		Abstract: make([]byte, 0),
	}
}

// Reset clears all information from this Document so it can be reused.
func (p *Document) Reset() {
	p.Id = p.Id[:0]
	p.Title = p.Title[:0]
	p.Url = p.Url[:0]
	p.Abstract = p.Abstract[:0]
	//p.links = p.links[:0]
}

// DocumentSerializer serializes a Document for writing
type DocumentSerializer interface {
	Serialize(p *Document, w io.Writer) error
}
