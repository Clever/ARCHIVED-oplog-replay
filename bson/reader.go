package bson

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
)

// mongodump outputs collections as binary files with no delimiter between documents. We need to:
//   1) Read 4 bytes (the size of the document, including these bytes)
//   2) Read the size - 4 bytes (the rest of the document)
//   3) Concatenate together these bytes
func New(r io.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(r)
	scanner.Split(func(data []byte, atEOF bool) (int, []byte, error) {
		needMoreData := func() (int, []byte, error) { return 0, nil, nil }
		r := bytes.NewBuffer(data)

		// 1)
		// We don't read straight into the size because we want to save these bytes for later, for
		// when we reconstruct the full document.
		sizeBytes := make([]byte, 4)
		if _, err := r.Read(sizeBytes); err == io.EOF {
			return needMoreData()
		} else if err != nil {
			return 0, nil, err
		}

		var size int32
		if err := binary.Read(bytes.NewBuffer(sizeBytes), binary.LittleEndian, &size); err != nil {
			return 0, nil, err
		}

		if int(size-4) > r.Len() {
			return needMoreData()
		}

		// 2)
		rest := make([]byte, size-4)
		if _, err := r.Read(rest); err != nil {
			return 0, nil, err
		}

		// 3)
		doc := bytes.Join([][]byte{sizeBytes, rest}, []byte{})

		return int(size), doc, nil
	})
	return scanner
}
