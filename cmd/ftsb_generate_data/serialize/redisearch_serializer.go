package serialize

import (
	"github.com/RediSearch/redisearch-go/redisearch"
	"io"
)

// RediSearchDocumentSerializer writes a WikiAbstract in a serialized form for RediSearch
type RediSearchDocumentSerializer struct{}

// Serialize writes WikiAbstract data to the given writer, in a format that will be easy to create a RediSearch command
func (s *RediSearchDocumentSerializer) Serialize(p *redisearch.Document, w io.Writer) (err error) {
	// To do this
	// FT.ADD myIdx doc1 1.0 FIELDS title "hello world" body "lorem ipsum" url "http://redis.io"
	// This function writes output that looks like:
	// <Id>,<Score>,<Property Name>=<Property Value>,<Property Name>=<Property Value>,...\n

	buf := make([]byte, 0, 1024)
	buf = append(buf, p.Id...)
	buf = append(buf, ',')
	buf = fastFormatAppend(p.Score, buf)
	buf = append(buf, ',')
	for key, value := range p.Properties {
		buf = append(buf, key...)
		buf = append(buf, '=')
		buf = fastFormatAppend(value, buf)
	}
	_, err = w.Write(buf)
	return err
}
