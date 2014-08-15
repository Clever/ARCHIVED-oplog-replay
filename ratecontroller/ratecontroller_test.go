package ratecontroller

import (
	"runtime/debug"
	"testing"
	"time"

	"labix.org/v2/mgo/bson"
)

func checkBooleanArg(t *testing.T, expected, actual bool) {
	if expected != actual {
		debug.PrintStack()
		t.Fatal("Expected does match actual")
	}
}

func TestRelativeRateController(t *testing.T) {
	startTime := int(time.Now().Unix())
	firstOp := map[string]interface{}{"ts": bson.MongoTimestamp(startTime << 32), "h": 1000, "v": 2, "op": "n", "ns": "", "o": map[string]interface{}{"message": "nop"}}
	controller := newRelativeRateController(20)

	// Try one op that should succeed
	checkBooleanArg(t, true, controller.ShouldApplyOp(firstOp))

	// 1 second passes in log processing time, but the next entry is 3 seconds later,
	// so even with the multipler of two we shouldn't process it
	secondOp := map[string]interface{}{"ts": bson.MongoTimestamp((startTime + 3) << 32), "h": 1000, "v": 2, "op": "n", "ns": "", "o": map[string]interface{}{"message": "nop"}}
	time.Sleep(time.Duration(100) * time.Millisecond)
	checkBooleanArg(t, false, controller.ShouldApplyOp(secondOp))

	// After another second it should be available for processing
	time.Sleep(time.Duration(100) * time.Millisecond)
	checkBooleanArg(t, true, controller.ShouldApplyOp(secondOp))

}

func TestConstantRateController(t *testing.T) {
	startTime := int(time.Now().Unix())
	op := map[string]interface{}{"ts": bson.MongoTimestamp(startTime << 32), "h": 1000, "v": 2, "op": "n", "ns": "", "o": map[string]interface{}{"message": "nop"}}

	controller := newFixedRateController(20)

	checkBooleanArg(t, true, controller.ShouldApplyOp(op))
	time.Sleep(time.Duration(100) * time.Millisecond)
	// Should accept two and then fail
	checkBooleanArg(t, true, controller.ShouldApplyOp(op))
	checkBooleanArg(t, true, controller.ShouldApplyOp(op))
	checkBooleanArg(t, false, controller.ShouldApplyOp(op))

	time.Sleep(time.Duration(100) * time.Millisecond)
	// Make sure it works a second time
	checkBooleanArg(t, true, controller.ShouldApplyOp(op))
	checkBooleanArg(t, true, controller.ShouldApplyOp(op))
	checkBooleanArg(t, false, controller.ShouldApplyOp(op))
}
