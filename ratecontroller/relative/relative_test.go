package relative

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"labix.org/v2/mgo/bson"
)

func TestRelativeRateController(t *testing.T) {
	startTime := int(time.Now().Unix())
	firstOp := map[string]interface{}{"ts": bson.MongoTimestamp(startTime << 32), "h": 1000, "v": 2, "op": "n", "ns": "", "o": map[string]interface{}{"message": "nop"}}
	controller := New(20)

	// Try one op that should succeed
	waitDuration := controller.WaitTime(firstOp)
	assert.Equal(t, 0, waitDuration.Nanoseconds())

	// 100ms passes in log processing time, but the next entry is 3 seconds later,
	// so even with the multipler of two we shouldn't process it
	secondOp := map[string]interface{}{"ts": bson.MongoTimestamp((startTime + 3) << 32), "h": 1000, "v": 2, "op": "n", "ns": "", "o": map[string]interface{}{"message": "nop"}}
	time.Sleep(time.Duration(100) * time.Millisecond)
	waitDuration = controller.WaitTime(secondOp)
	if waitDuration.Seconds() > 0.1 || waitDuration.Seconds() <= 0.0 {
		t.Fatalf("Wait duration not in range of (0.0, 100] ms. Is: %f", waitDuration.Seconds())
	}

	// After another 100ms it should be available for processing
	time.Sleep(time.Duration(100) * time.Millisecond)
	waitDuration = controller.WaitTime(secondOp)
	assert.Equal(t, 0, waitDuration.Nanoseconds())
}
