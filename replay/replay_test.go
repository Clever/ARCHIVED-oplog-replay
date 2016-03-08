package replay

import (
	"fmt"
	"math"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/Clever/oplog-replay/ratecontroller/relative"
	"github.com/stretchr/testify/assert"

	"labix.org/v2/mgo"
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
				return fmt.Errorf("Expected op: %#v, got: %#v\n", ops[nextExpectedOp], op)
			}
			receivedTime := int(math.Floor(time.Now().Sub(startTime).Seconds() + 0.5))
			if receivedTime != expectedTimes[nextExpectedOp] {
				return fmt.Errorf("Got correct op, but expected it after %v second(s). Got it after %v second(s).\n", expectedTimes[nextExpectedOp], receivedTime)
			}
			nextExpectedOp++
		}
		return nil
	}

	done := make(chan struct{})
	opChannel := make(chan map[string]interface{})
	go func() {
		for _, op := range ops {
			opChannel <- op
		}
		close(opChannel)
	}()

	timedOps := controlRate(done, opChannel, relative.New(1))
	batchedOps := batchOps(done, timedOps)
	if err := oplogReplay(batchedOps, applyOps); err != nil {
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
				return fmt.Errorf("Got correct op, but expected it after %v second(s). Got it after %v second(s).\n", expectedTimes[nextExpectedOp], receivedTime)
			}
			nextExpectedOp++
		}
		return nil
	}

	done := make(chan struct{})
	opChannel := make(chan map[string]interface{})
	go func() {
		for _, op := range ops {
			opChannel <- op
		}
		close(opChannel)
	}()
	timedOps := controlRate(done, opChannel, relative.New(5))
	batchedOps := batchOps(done, timedOps)
	if err := oplogReplay(batchedOps, applyOps); err != nil {
		t.Fatalf(err.Error())
	}
}

func TestWillApplyInBatch(t *testing.T) {
	ops := []map[string]interface{}{
		map[string]interface{}{"ts": bson.MongoTimestamp(0 << 32), "h": 1000, "v": 2, "op": "i", "ns": "testdb.test", "o": map[string]interface{}{"some": "insert"}},
		map[string]interface{}{"ts": bson.MongoTimestamp(10 << 32), "h": 1001, "v": 2, "op": "i", "ns": "testdb.test", "o": map[string]interface{}{"some": "insert"}},
		map[string]interface{}{"ts": bson.MongoTimestamp(10 << 32), "h": 1002, "v": 2, "op": "i", "ns": "testdb.test", "o": map[string]interface{}{"some": "insert"}},
	}

	// This test makes sure that entries are being applied in batch. In particular it simulates
	// the applyOps function taking a while by having it wait until the operation generator can
	// push a bunch of new elements in the channel before returning from applying the op.
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
			// It should try to apply two operations here because the channel put in
			// two new elements before this completed the first time.
			if len(ops) != 2 {
				return fmt.Errorf("Expected 2 ops the second time, got %x", len(ops))
			}
			return nil
		}
	}

	done := make(chan struct{})
	opChannel := make(chan map[string]interface{})

	go func() {
		opChannel <- ops[0]
		// Wait for the applyOps function to process the first
		<-opLogGeneratorWaiter
		opChannel <- ops[1]
		opChannel <- ops[2]
		// Tell the applyOps function that it can finish the first apply now that there
		// are two more operations in the channel.
		applyOpsWaiter <- true
		close(opChannel)
	}()

	timedOps := controlRate(done, opChannel, relative.New(100))
	batchedOps := batchOps(done, timedOps)
	if err := oplogReplay(batchedOps, applyOps); err != nil {
		t.Fatal(err.Error())
	}
}

func setupTestDb(t *testing.T) (*mgo.Session, *mgo.Collection) {
	mongoURL := os.Getenv("MONGO_URL")
	if len(mongoURL) == 0 {
		mongoURL = "localhost"
	}
	session, err := mgo.Dial(mongoURL)
	assert.Nil(t, err)

	replayTestDb := session.DB("testdb").C("replayTest")
	replayTestDb.Remove(bson.M{"_id": "missingUpdate"})
	replayTestDb.Remove(bson.M{"insertKey": "value"})

	return session, replayTestDb
}

func getUpdateToNonExistentOp() map[string]interface{} {
	return map[string]interface{}{"ts": bson.MongoTimestamp(15 << 32), "h": 1003, "v": 2, "op": "u", "ns": "testdb.replayTest", "o": map[string]interface{}{"some": "update"}, "o2": map[string]interface{}{"_id": "missingUpdate"}}
}

func getSuccessfulUpsertOp() map[string]interface{} {
	return map[string]interface{}{"ts": bson.MongoTimestamp(15 << 32), "h": 1003, "v": 2, "op": "i", "ns": "testdb.replayTest", "o": map[string]interface{}{"insertKey": "value"}, "o2": map[string]interface{}{"_id": "correctInsert"}}
}

func TestUpdateNonExistentDocShouldFail(t *testing.T) {
	session, replayTestDb := setupTestDb(t)
	defer session.Close()
	done := make(chan struct{})
	opChannel := make(chan map[string]interface{}, 1)
	opChannel <- getUpdateToNonExistentOp()
	close(opChannel)

	timedOps := controlRate(done, opChannel, relative.New(100))
	batchedOps := batchOps(done, timedOps)

	err := oplogReplay(batchedOps, getApplyOpsFunc(session, false))
	assert.NotNil(t, err)
	failedOpError, ok := err.(*FailedOperationError)
	assert.True(t, ok, "Wrong error type returned")
	assert.Equal(t, failedOpError.op, getUpdateToNonExistentOp())

	// Check that the element isn't in the db
	var result interface{}
	err = replayTestDb.Find(bson.M{"_id": "missingUpdate"}).One(&result)
	assert.EqualError(t, err, "not found")
}

func TestUpdateNonExistentDocShouldUpsertWithFlagSet(t *testing.T) {
	session, replayTestDb := setupTestDb(t)
	defer session.Close()
	done := make(chan struct{})
	opChannel := make(chan map[string]interface{}, 1)
	opChannel <- getUpdateToNonExistentOp()
	close(opChannel)

	timedOps := controlRate(done, opChannel, relative.New(100))
	batchedOps := batchOps(done, timedOps)

	err := oplogReplay(batchedOps, getApplyOpsFunc(session, true))
	assert.Nil(t, err)

	// Check that the element is in the db
	var result map[string]interface{}
	err = replayTestDb.Find(bson.M{"_id": "missingUpdate"}).One(&result)
	assert.Nil(t, err)
	assert.Equal(t, "missingUpdate", result["_id"])
}

func TestOneBadOperationFailsReplay(t *testing.T) {
	session, replayTestDb := setupTestDb(t)
	defer session.Close()

	// Do two operations. One should fail, the other should succeed
	done := make(chan struct{})
	opChannel := make(chan map[string]interface{}, 2)
	opChannel <- getSuccessfulUpsertOp()
	opChannel <- getUpdateToNonExistentOp()
	close(opChannel)

	timedOps := controlRate(done, opChannel, relative.New(100))
	batchedOps := batchOps(done, timedOps)
	err := oplogReplay(batchedOps, getApplyOpsFunc(session, false))
	assert.NotNil(t, err)
	failedOpError, ok := err.(*FailedOperationError)
	assert.True(t, ok, "Wrong error type returned")
	assert.Equal(t, failedOpError.op, getUpdateToNonExistentOp())

	var result map[string]interface{}
	err = replayTestDb.Find(bson.M{"insertKey": "value"}).One(&result)
	assert.Nil(t, err)
	assert.Equal(t, "value", result["insertKey"])

	err = replayTestDb.Find(bson.M{"_id": "missingUpdate"}).One(&result)
	assert.EqualError(t, err, "not found")
}

func TestASuccessfulOplogOperation(t *testing.T) {
	session, replayTestDb := setupTestDb(t)
	defer session.Close()

	done := make(chan struct{})
	opChannel := make(chan map[string]interface{}, 1)
	opChannel <- getSuccessfulUpsertOp()
	close(opChannel)

	timedOps := controlRate(done, opChannel, relative.New(100))
	batchedOps := batchOps(done, timedOps)

	err := oplogReplay(batchedOps, getApplyOpsFunc(session, false))
	assert.Nil(t, err)

	var result map[string]interface{}
	err = replayTestDb.Find(bson.M{"insertKey": "value"}).One(&result)
	assert.Nil(t, err)
	assert.Equal(t, "value", result["insertKey"])
}
