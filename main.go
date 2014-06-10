package main

import (
	// "bytes"
	// "encoding/binary"
	"flag"
	"fmt"
	bsonScanner "github.com/Clever/oplog-replay/bson"
	"io"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"os"
)

func parseBSON(r io.Reader) ([]map[string]interface{}, error) {

	ops := []map[string]interface{}{}

	scanner := bsonScanner.New(r)
	for scanner.Scan() {
		op := map[string]interface{}{}
		if err := bson.Unmarshal(scanner.Bytes(), &op); err != nil {
			return nil, err
		}
		ops = append(ops, op)
	}
	if scanner.Err() != nil {
		return nil, scanner.Err()
	}
	return ops, nil
}

func main() {
	// speed := flag.Float64("speed", 1, "Sets multiplier for playback speed.")
	host := flag.String("host", "localhost", "Mongo host to playback onto.")
	flag.Parse()

	session, err := mgo.Dial(*host)
	if err != nil {
		panic(err)
	}
	defer session.Close()

	fmt.Println("Starting playback...")
	ops, err := parseBSON(os.Stdin)
	if err != nil {
		panic(err)
	}

	for _, op := range ops {
		if op["ns"] == "" {
			// Can't apply ops without a db name
			continue
		}
		opArray := []interface{}{op}
		var result interface{}

		if err := session.Run(bson.M{"applyOps": opArray}, &result); err != nil {
			panic(err)
		}
	}

	fmt.Printf("Done! Read %d ops\n", len(ops))
}
