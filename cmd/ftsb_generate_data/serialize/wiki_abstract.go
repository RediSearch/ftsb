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
type WikiAbstract struct {
	Id, Title, Url, Abstract []byte
}

// NewWikiAbstract returns a new empty WikiAbstract
func NewWikiAbstract() *WikiAbstract {
	return &WikiAbstract{
		Id:       make([]byte, 0),
		Title:    make([]byte, 0),
		Url:      make([]byte, 0),
		Abstract: make([]byte, 0),
	}
}

// Reset clears all information from this WikiAbstract so it can be reused.
func (p *WikiAbstract) Reset() {
	p.Id = p.Id[:0]
	p.Title = p.Title[:0]
	p.Url = p.Url[:0]
	p.Abstract = p.Abstract[:0]
}

// DocumentSerializer serializes a WikiAbstract for writing
type DocumentSerializer interface {
	Serialize(p *WikiAbstract, w io.Writer) error
}
