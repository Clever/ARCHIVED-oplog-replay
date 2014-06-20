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
	"time"
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

func oplogReplay(ops []map[string]interface{}, applyOp func(interface{}) error, speed float64) error {
	if speed == -1 {
		for _, op := range ops {
			if err := applyOp(op); err != nil {
				return err
			}
		}
		return nil
	}
	logStartTime := -1
	replayStartTime := time.Now()
	for _, op := range ops {
		if op["ns"] == "" {
			// Can't apply ops without a db name
			continue
		}

		eventTime := int((op["ts"].(bson.MongoTimestamp)) >> 32)

		if logStartTime == -1 {
			logStartTime = eventTime
		}

		for time.Now().Sub(replayStartTime).Seconds()*speed < float64(eventTime-logStartTime) {
			time.Sleep(time.Duration(10) * time.Millisecond)
		}

		if err := applyOp(op); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	speed := flag.Float64("speed", 1, "Sets multiplier for playback speed.")
	host := flag.String("host", "localhost", "Mongo host to playback onto.")
	flag.Parse()

	fmt.Println("Starting playback...")
	ops, err := parseBSON(os.Stdin)
	if err != nil {
		panic(err)
	}

	session, err := mgo.Dial(*host)
	if err != nil {
		panic(err)
	}
	defer session.Close()

	applyOp := func(op interface{}) error {
		var result interface{}
		if err := session.Run(bson.M{"applyOps": []interface{}{op}}, &result); err != nil {
			return err
		}
		return nil
	}

	if err := oplogReplay(ops, applyOp, *speed); err != nil {
		panic(err)
	}

	fmt.Printf("Done! Read %d ops\n", len(ops))
}
