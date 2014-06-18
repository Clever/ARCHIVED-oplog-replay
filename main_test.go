package main

import (
	"fmt"
	"labix.org/v2/mgo/bson"
	"math"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestParseBSON(t *testing.T) {
	expected := []map[string]interface{}{
		map[string]interface{}{"ts": 6021954198109683713, "h": 920013897904662416, "v": 2, "op": "c", "ns": "testdb.$cmd", "o": map[string]interface{}{"create": "test"}},
		map[string]interface{}{"ts": 6021954253944258561, "h": -7024883673281943103, "v": 2, "op": "i", "ns": "testdb.test", "o": map[string]interface{}{"_id": "S\x92G}S\xa5\xb2\x9c\x16\xf84\xf1", "message": "insert test", "number": 1}},
		map[string]interface{}{"ts": 6021954314073800705, "h": 8562537077519333892, "v": 2, "op": "i", "ns": "testdb.test", "o": map[string]interface{}{"_id": "S\x92G\x8bS\xa5\xb2\x9c\x16\xf84\xf2", "message": "update test", "number": 2}},
		map[string]interface{}{"ts": 6021954326958702593, "h": 4976203120731500765, "v": 2, "op": "i", "ns": "testdb.test", "o": map[string]interface{}{"_id": "S\x92G\x95S\xa5\xb2\x9c\x16\xf84\xf3", "message": "delete test", "number": 3}},
		map[string]interface{}{"ts": 6021954408563081217, "h": 5650666146636305048, "v": 2, "op": "u", "ns": "testdb.test", "o2": map[string]interface{}{"_id": "S\x92G\x8bS\xa5\xb2\x9c\x16\xf84\xf2"}, "o": map[string]interface{}{"_id": "S\x92G\x8bS\xa5\xb2\x9c\x16\xf84\xf2", "message": "update test", "number": 5}},
		map[string]interface{}{"ts": 6021954451512754177, "h": -4953188477403348903, "v": 2, "op": "d", "ns": "testdb.test", "b": true, "o": map[string]interface{}{"_id": "S\x92G\x95S\xa5\xb2\x9c\x16\xf84\xf3"}},
	}

	f, _ := os.Open("./oplog.rs.bson")
	defer f.Close()

	parsed, _ := parseBSON(f)

	if fmt.Sprintf("%#v", expected) != fmt.Sprintf("%#v", parsed) {
		t.Fatal("BSON did not parse correctly!")
	}
}

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
	applyOp := func(op interface{}) {
		if !reflect.DeepEqual(ops[nextExpectedOp], op) {
			t.Fatalf("Expected op: %#v, got: %#v\n", ops[nextExpectedOp], ops)
		}

		receivedTime := int(math.Floor(time.Now().Sub(startTime).Seconds() + 0.5))
		if receivedTime != expectedTimes[nextExpectedOp] {
			t.Fatalf("Got correct op, but expected it after %v second(s). Got it after %v second(s).\n", expectedTimes[nextExpectedOp], receivedTime)
		}
		nextExpectedOp++
	}

	oplogReplay(ops, applyOp, 1)

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
	applyOp := func(op interface{}) {
		receivedTime := int(math.Floor(time.Now().Sub(startTime).Seconds() + 0.5))
		if receivedTime != expectedTimes[nextExpectedOp] {
			t.Fatalf("Got correct op, but expected it after %v second(s). Got it after %v second(s).\n", expectedTimes[nextExpectedOp], receivedTime)
		}
		nextExpectedOp++
	}

	oplogReplay(ops, applyOp, 5)
}
