package fixed

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"labix.org/v2/mgo/bson"
)

func TestRateController(t *testing.T) {
	startTime := time.Now().Unix()
	op := map[string]interface{}{
		"ts": bson.MongoTimestamp(startTime << 32),
		"h":  1000,
		"v":  2,
		"op": "n",
		"ns": "",
		"o":  map[string]interface{}{"message": "nop"}}

	controller := New(10)

	waitDuration := controller.WaitTime(op)
	// Should be 0 for the first call
	assert.Equal(t, int64(0), waitDuration.Nanoseconds())
	waitDuration = controller.WaitTime(op)
	if waitDuration.Seconds() > 0.1 || waitDuration.Seconds() <= 0.0 {
		t.Fatalf("Wait duration not in range of (0.0, 0.1] secs. Is: %f", waitDuration.Seconds())
	}

	// After 200ms should be able to apply one more
	time.Sleep(time.Duration(200) * time.Millisecond)
	waitDuration = controller.WaitTime(op)
	assert.Equal(t, int64(0), waitDuration.Nanoseconds())

	// But not two more
	waitDuration = controller.WaitTime(op)
	if waitDuration.Seconds() > 0.1 || waitDuration.Seconds() <= 0.0 {
		t.Fatalf("Wait duration not in range of (0.0, 0.1] secs. Is: %f", waitDuration.Seconds())
	}
}
