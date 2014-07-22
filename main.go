package main

import (
	"flag"
	"fmt"
	bsonScanner "github.com/Clever/oplog-replay/bson"
	"io"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"math"
	"os"
	"time"
)

func parseBSON(r io.Reader, opChannel chan map[string]interface{}) {
	defer close(opChannel)

	scanner := bsonScanner.New(r)
	for scanner.Scan() {
		op := map[string]interface{}{}
		if err := bson.Unmarshal(scanner.Bytes(), &op); err != nil {
			panic(err)
		}
		opChannel <- op
	}
	if scanner.Err() != nil {
		panic(scanner.Err())
	}
}

func oplogReplay(ops chan map[string]interface{}, applyOp func(interface{}) error, speed float64) error {
	timedOps := make(chan map[string]interface{})
	errors := make(chan error)
	go func() {
		defer close(errors)
		for op := range timedOps {
			if err := applyOp(op); err != nil {
				errors <- err
			}
		}
	}()
	logStartTime := -1
	replayStartTime := time.Now()
	for op := range ops {
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

		timedOps <- op
	}
	close(timedOps)
	for err := range errors {
		return err
	}
	return nil
}

func main() {
	speed := flag.Float64("speed", 1, "Sets multiplier for playback speed.")
	host := flag.String("host", "localhost", "Mongo host to playback onto.")
	flag.Parse()

	fmt.Println("Parsing BSON...")
	opChannel := make(chan map[string]interface{})
	go parseBSON(os.Stdin, opChannel)

	session, err := mgo.Dial(*host)
	if err != nil {
		panic(err)
	}
	defer session.Close()

	applyOp := func(op interface{}) error {
		var result interface{}
		session.Refresh()
		if err := session.Run(bson.M{"applyOps": []interface{}{op}}, &result); err != nil {
			return err
		}
		return nil
	}

	fmt.Println("Begin replaying...")

	if *speed == -1 {
		*speed = math.Inf(1)
	}
	if err := oplogReplay(opChannel, applyOp, *speed); err != nil {
		panic(err)
	}

	fmt.Println("Done!")
}
