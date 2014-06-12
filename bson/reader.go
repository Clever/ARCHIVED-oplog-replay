package bson

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
)

func needMoreData() (int, []byte, error) { return 0, nil, nil }

// mongodump outputs collections as binary files with all the documents appended together.
// The first four bytes are the size of the full document, including the size bytes.
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

		return int(size), data[0:size], nil
	})
	return scanner
}
