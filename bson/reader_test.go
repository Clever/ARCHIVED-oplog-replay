package bson

import (
	"os"
	"reflect"
	"testing"

	"labix.org/v2/mgo/bson"
)

func TestParseBSON(t *testing.T) {
	expected := []map[string]interface{}{
		map[string]interface{}{"ts": bson.MongoTimestamp(6021954198109683713), "h": int64(920013897904662416), "v": 2, "op": "c", "ns": "testdb.$cmd", "o": map[string]interface{}{"create": "test"}},
		map[string]interface{}{"ts": bson.MongoTimestamp(6021954253944258561), "h": int64(-7024883673281943103), "v": 2, "op": "i", "ns": "testdb.test", "o": map[string]interface{}{"_id": bson.ObjectIdHex("5392477d53a5b29c16f834f1"), "message": "insert test", "number": 1}},
		map[string]interface{}{"ts": bson.MongoTimestamp(6021954314073800705), "h": int64(8562537077519333892), "v": 2, "op": "i", "ns": "testdb.test", "o": map[string]interface{}{"_id": bson.ObjectIdHex("5392478b53a5b29c16f834f2"), "message": "update test", "number": 2}},
		map[string]interface{}{"ts": bson.MongoTimestamp(6021954326958702593), "h": int64(4976203120731500765), "v": 2, "op": "i", "ns": "testdb.test", "o": map[string]interface{}{"_id": bson.ObjectIdHex("5392479553a5b29c16f834f3"), "message": "delete test", "number": 3}},
		map[string]interface{}{"ts": bson.MongoTimestamp(6021954408563081217), "h": int64(5650666146636305048), "v": 2, "op": "u", "ns": "testdb.test", "o2": map[string]interface{}{"_id": bson.ObjectIdHex("5392478b53a5b29c16f834f2")}, "o": map[string]interface{}{"_id": bson.ObjectIdHex("5392478b53a5b29c16f834f2"), "message": "update test", "number": 5}},
		map[string]interface{}{"ts": bson.MongoTimestamp(6021954451512754177), "h": int64(-4953188477403348903), "v": 2, "op": "d", "ns": "testdb.test", "b": true, "o": map[string]interface{}{"_id": bson.ObjectIdHex("5392479553a5b29c16f834f3")}},
	}

	f, err := os.Open("./testdata.bson")
	if err != nil {
		t.Fatal("Got error", err)
	}
	defer f.Close()

	nextOpIndex := 0
	scanner := New(f)
	for scanner.Scan() {
		op := map[string]interface{}{}
		if err := bson.Unmarshal(scanner.Bytes(), &op); err != nil {
			t.Fatal("Got error in unmarshalling: ", err)
		}

		if !reflect.DeepEqual(op, expected[nextOpIndex]) {
			t.Fatal("Op did not match expected!")
		}
		nextOpIndex++
	}
	if scanner.Err() != nil {
		t.Fatal("Scanner error", scanner.Err())
	}

	if nextOpIndex != 6 {
		t.Fatal("Did not see all ops!", nextOpIndex)
	}
}

func TestParseLargeBSON(t *testing.T) {
	largeArray := make([]interface{}, 5000)
	for i := 0; i < 5000; i++ {
		largeArray[i] = float64(i)
	}
	expectedOp := map[string]interface{}{
		"ts": bson.MongoTimestamp(6048257058866724865), "h": int64(-6825742652110581687), "v": 2, "op": "i", "ns": "testdb.testdb", "o": map[string]interface{}{
			"_id": bson.ObjectIdHex("53efb9c067fd92348e823860"),
			"val": largeArray}}

	f, err := os.Open("./largetestdata.bson")
	if err != nil {
		t.Fatal("Error loading file", err)
	}
	defer f.Close()
	foundExpectedOp := false
	scanner := New(f)
	for scanner.Scan() {
		op := map[string]interface{}{}
		if err := bson.Unmarshal(scanner.Bytes(), &op); err != nil {
			t.Fatal("Error unmarshalling: ", err)
		}
		if reflect.DeepEqual(op, expectedOp) {
			foundExpectedOp = true
		}
	}
	if scanner.Err() != nil {
		t.Fatal("Scanner error: ", scanner.Err())
	}
	if !foundExpectedOp {
		t.Fatal("Didn't find the expected operation")
	}

}
