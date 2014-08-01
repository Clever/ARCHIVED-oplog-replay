package replay

import (
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
	applyOp := func(op interface{}) error {
		if !reflect.DeepEqual(ops[nextExpectedOp], op) {
			t.Fatalf("Expected op: %#v, got: %#v\n", ops[nextExpectedOp], ops)
		}

		receivedTime := int(math.Floor(time.Now().Sub(startTime).Seconds() + 0.5))
		if receivedTime != expectedTimes[nextExpectedOp] {
			t.Fatalf("Got correct op, but expected it after %v second(s). Got it after %v second(s).\n", expectedTimes[nextExpectedOp], receivedTime)
		}
		nextExpectedOp++
		return nil
	}

	opChannel := make(chan map[string]interface{})
	go func() {
		for _, op := range ops {
			opChannel <- op
		}
		close(opChannel)
	}()
	oplogReplay(opChannel, applyOp, 1)

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
	applyOp := func(op interface{}) error {
		receivedTime := int(math.Floor(time.Now().Sub(startTime).Seconds() + 0.5))
		if receivedTime != expectedTimes[nextExpectedOp] {
			t.Fatalf("Got correct op, but expected it after %v second(s). Got it after %v second(s).\n", expectedTimes[nextExpectedOp], receivedTime)
		}
		nextExpectedOp++
		return nil
	}

	opChannel := make(chan map[string]interface{})
	go func() {
		for _, op := range ops {
			opChannel <- op
		}
		close(opChannel)
	}()
	oplogReplay(opChannel, applyOp, 5)
}
