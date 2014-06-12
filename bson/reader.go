package bson

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
)

func needMoreData() (int, []byte, error) { return 0, nil, nil }

// mongodump outputs collections as binary files with all the documents appended together. We need to:
//   1) Read 4 bytes (the size of the document, including these bytes)
//   2) Read the size - 4 bytes (the rest of the document)
//   3) Concatenate together these bytes to get the full document
func New(r io.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(r)
	scanner.Split(func(data []byte, atEOF bool) (int, []byte, error) {
		if len(data) < 4 {
			return needMoreData()
		}
		sizeBytes := data[0:4]

		var size int32
		if err := binary.Read(bytes.NewBuffer(sizeBytes), binary.LittleEndian, &size); err != nil {
			return 0, nil, err
		}

		if int(size) > len(data) {
			return needMoreData()
		}

		doc := data[0:size]
		return int(size), doc, nil
	})
	return scanner
}
