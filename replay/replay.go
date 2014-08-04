package replay

import (
	"fmt"
	"io"
	"math"

	bsonScanner "github.com/Clever/oplog-replay/bson"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"

	"time"
)

// ParseBSON parses the bson from the Reader interface. It writes each operation to the opChannel.
// If there are any errors it closes the opChannel are returns immediately.
func parseBSON(r io.Reader, opChannel chan map[string]interface{}) error {
	defer close(opChannel)

	scanner := bsonScanner.New(r)
	for scanner.Scan() {
		op := map[string]interface{}{}
		if err := bson.Unmarshal(scanner.Bytes(), &op); err != nil {
			return err
		}
		opChannel <- op
	}
	if scanner.Err() != nil {
		return scanner.Err()
	}
	return nil
}

func oplogReplay(ops chan map[string]interface{}, applyOp func(interface{}) error, speed float64) error {
	timedOps := make(chan map[string]interface{})
	// Run a goroutine that applies the ops. If there are any errors in application this returns immediately.
	// It sets the timedOpsReturnVal channel with the error response.
	timedOpsReturnVal := make(chan error, 1)
	go func() {
		for op := range timedOps {
			if err := applyOp(op); err != nil {
				timedOpsReturnVal <- err
				return
			}
		}
		timedOpsReturnVal <- nil
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
	return <-timedOpsReturnVal
}

// ReplayOplog replays an oplog onto the specified host
func ReplayOplog(r io.Reader, speed float64, host string) error {
	fmt.Println("Parsing BSON...")
	opChannel := make(chan map[string]interface{})
	parseBSONReturnVal := make(chan error, 1)
	go func() {
		parseBSONReturnVal <- parseBSON(r, opChannel)
	}()
	session, err := mgo.Dial(host)
	if err != nil {
		return err
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

	if speed == -1 {
		speed = math.Inf(1)
	}
	err = oplogReplay(opChannel, applyOp, speed)
	if err != nil {
		return err
	}
	return <-parseBSONReturnVal
}
