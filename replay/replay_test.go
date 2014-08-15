package replay

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"labix.org/v2/mgo/bson"
)

func TestOplogReplay(t *testing.T) {
	ops := []map[string]interface{}{
		map[string]interface{}{"ts": bson.MongoTimestamp(10 << 32), "h": 1000, "v": 2, "op": "n", "ns": "", "o": map[string]interface{}{"message": "nop"}},
		map[string]interface{}{"ts": bson.MongoTimestamp(11 << 32), "h": 1001, "v": 2, "op": "c", "ns": "testdb.$cmd", "o": map[string]interface{}{"create": "test"}},
		map[string]interface{}{"ts": bson.MongoTimestamp(12 << 32), "h": 1002, "v": 2, "op": "i", "ns": "testdb.test", "o": map[string]interface{}{"some": "insert"}},
		map[string]interface{}{"ts": bson.MongoTimestamp(15 << 32), "h": 1003, "v": 2, "op": "u", "ns": "testdb.test", "o": map[string]interface{}{"some": "update"}, "o2": map[string]interface{}{"some": "update2"}},
		map[string]interface{}{"ts": bson.MongoTimestamp(15 << 32), "h": 1004, "v": 2, "op": "d", "ns": "testdb.test", "o": map[string]interface{}{"some": "delete"}, "b": true},
		map[string]interface{}{"ts": bson.MongoTimestamp(16 << 32), "h": 1005, "v": 2, "op": "d", "ns": "testdb.$cmd", "o": map[string]interface{}{"create": "test2"}},
	}

	expectedTimes := []int{-1, 0, 1, 4, 4, 5}
	// The first op is a nop, so the first one expected is the second one.
	nextExpectedOp := 1

	startTime := time.Now()
	applyOps := func(opList []interface{}) error {
		for _, op := range opList {
			if !reflect.DeepEqual(ops[nextExpectedOp], op) {
				return errors.New(fmt.Sprintf("Expected op: %#v, got: %#v\n", ops[nextExpectedOp], op))
			}
			receivedTime := int(math.Floor(time.Now().Sub(startTime).Seconds() + 0.5))
			if receivedTime != expectedTimes[nextExpectedOp] {
				t.Fatalf("Got correct op, but expected it after %v second(s). Got it after %v second(s).\n", expectedTimes[nextExpectedOp], receivedTime)
			}
			nextExpectedOp++
		}
		return nil
	}

	opChannel := make(chan map[string]interface{})
	go func() {
		for _, op := range ops {
			opChannel <- op
		}
		close(opChannel)
	}()
	err := oplogReplay(opChannel, applyOps, 1)
	if err != nil {
		t.Fatal(err.Error())
	}

	if nextExpectedOp-1 != 5 {
		t.Fatalf("Did not get all ops, expected 5, got %v\n", nextExpectedOp-1)
	}
}

func TestOplogReplaySpeed(t *testing.T) {
	ops := []map[string]interface{}{
		map[string]interface{}{"ts": bson.MongoTimestamp(0 << 32), "h": 1000, "v": 2, "op": "i", "ns": "testdb.test", "o": map[string]interface{}{"some": "insert"}},
		map[string]interface{}{"ts": bson.MongoTimestamp(10 << 32), "h": 1001, "v": 2, "op": "i", "ns": "testdb.test", "o": map[string]interface{}{"some": "insert"}},
	}

	expectedTimes := []int{0, 2}
	nextExpectedOp := 0

	startTime := time.Now()
	applyOps := func(ops []interface{}) error {
		for _ = range ops {
			receivedTime := int(math.Floor(time.Now().Sub(startTime).Seconds() + 0.5))
			if receivedTime != expectedTimes[nextExpectedOp] {
				errors.New(fmt.Sprintf("Got correct op, but expected it after %v second(s). Got it after %v second(s).\n", expectedTimes[nextExpectedOp], receivedTime))
			}
			nextExpectedOp++
		}
		return nil
	}

	opChannel := make(chan map[string]interface{})
	go func() {
		for _, op := range ops {
			opChannel <- op
		}
		close(opChannel)
	}()
	if err := oplogReplay(opChannel, applyOps, 5); err != nil {
		t.Fatalf(err.Error())
	}
}

func TestWillApplyInBatch(t *testing.T) {
	ops := []map[string]interface{}{
		map[string]interface{}{"ts": bson.MongoTimestamp(0 << 32), "h": 1000, "v": 2, "op": "i", "ns": "testdb.test", "o": map[string]interface{}{"some": "insert"}},
		map[string]interface{}{"ts": bson.MongoTimestamp(10 << 32), "h": 1001, "v": 2, "op": "i", "ns": "testdb.test", "o": map[string]interface{}{"some": "insert"}},
		map[string]interface{}{"ts": bson.MongoTimestamp(10 << 32), "h": 1002, "v": 2, "op": "i", "ns": "testdb.test", "o": map[string]interface{}{"some": "insert"}},
	}

	opLogGeneratorWaiter := make(chan bool)
	applyOpsWaiter := make(chan bool)
	firstApply := true
	applyOps := func(ops []interface{}) error {
		if firstApply {
			firstApply = false
			// Tell the oplog generator that it can make more
			opLogGeneratorWaiter <- true
			// Wait for a couple more to be added to the queue
			<-applyOpsWaiter
			return nil
		} else {
			if len(ops) != 2 {
				return errors.New(fmt.Sprintf("Expected 2 ops the second time, got %x", len(ops)))
			}
			return nil
		}
	}

	opChannel := make(chan map[string]interface{})
	go func() {
		opChannel <- ops[0]
		// TODO: Add a nice comment
		<-opLogGeneratorWaiter
		opChannel <- ops[1]
		opChannel <- ops[2]
		applyOpsWaiter <- true
		close(opChannel)
	}()

	if err := oplogReplay(opChannel, applyOps, 100); err != nil {
		t.Fatal(err.Error())
	}
}
