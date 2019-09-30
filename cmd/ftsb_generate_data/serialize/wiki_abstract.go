package serialize

// WikiAbstract wraps a single document. It stores database-agnostic data
import (
	"github.com/RediSearch/redisearch-go/redisearch"
	"io"
)

// WikiAbstract wraps a single document. It stores database-agnostic data
// representing one WikiAbstract
//
// Internally, WikiAbstract uses byte slices instead of strings to try to minimize
// overhead.

// DocumentSerializer serializes a WikiAbstract for writing
type DocumentSerializer interface {
	Serialize(p *redisearch.Document, w io.Writer) error
}
