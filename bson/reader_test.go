package bson

import (
	"fmt"
	"labix.org/v2/mgo/bson"
	"os"
	"testing"
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

		if fmt.Sprintf("%#v", op) != fmt.Sprintf("%#v", expected[nextOpIndex]) {
			t.Fatal("Op did not match expected!")
		}
		nextOpIndex++
	}

	if nextOpIndex != 6 {
		t.Fatal("Did not see all ops!", nextOpIndex)
	}
}
