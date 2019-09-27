package serialize

import (
	"io"
)

// RediSearchWikiAbstractSerializer writes a WikiAbstract in a serialized form for RediSearch
type RediSearchWikiPagesSerializer struct{}

// Serialize writes WikiAbstract data to the given writer, in a format that will be easy to create a RediSearch command
func (s *RediSearchWikiPagesSerializer) Serialize(p *WikiPages, w io.Writer) (err error) {
	// To do this
	// FT.ADD myIdx doc1 1.0 FIELDS title "hello world" body "lorem ipsum" url "http://redis.io"
	// #fields,lenField1,lenField2,(...) myIdx(implicit since the tool will use the index it wants) doc1 1.0 FIELDS(implicit) title "hello world" body "lorem ipsum" url "http://redis.io"
	//var buf []byte
	//var fieldStartAndSize []int
	//fieldStartAndSizeStr := ""
	//priorBufPos := 0
	//buf = append(buf, p.Id...)
	//fieldStartAndSize = append(fieldStartAndSize, len(buf)-priorBufPos)
	//priorBufPos = len(buf)
	//buf = append(buf, []byte("1.0")...)
	//fieldStartAndSize = append(fieldStartAndSize, len(buf)-priorBufPos)
	//priorBufPos = len(buf)
	//buf = append(buf, []byte("TITLE")...)
	//fieldStartAndSize = append(fieldStartAndSize, len(buf)-priorBufPos)
	//priorBufPos = len(buf)
	//buf = append(buf, p.Title...)
	//fieldStartAndSize = append(fieldStartAndSize, len(buf)-priorBufPos)
	//priorBufPos = len(buf)
	//buf = append(buf, []byte("URL")...)
	//fieldStartAndSize = append(fieldStartAndSize, len(buf)-priorBufPos)
	//priorBufPos = len(buf)
	//buf = append(buf, p.Url...)
	//fieldStartAndSize = append(fieldStartAndSize, len(buf)-priorBufPos)
	//priorBufPos = len(buf)
	//buf = append(buf, []byte("ABSTRACT")...)
	//fieldStartAndSize = append(fieldStartAndSize, len(buf)-priorBufPos)
	//priorBufPos = len(buf)
	//buf = append(buf, p.Abstract...)
	//fieldStartAndSize = append(fieldStartAndSize, len(buf)-priorBufPos)
	//buf = append(buf, []byte("\n")...)
	//
	//fieldStartAndSizeStr = fieldStartAndSizeStr + strconv.Itoa(len(fieldStartAndSize))
	//for _, fieldLen := range fieldStartAndSize {
	//	fieldStartAndSizeStr = fieldStartAndSizeStr + "," + strconv.Itoa(fieldLen)
	//}
	//fieldStartAndSizeStr = fieldStartAndSizeStr + ","
	//buf = append([]byte(fieldStartAndSizeStr), buf...)
	//
	//_, err = w.Write(buf)

	return err
}
