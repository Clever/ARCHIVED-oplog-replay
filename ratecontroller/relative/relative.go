package relative

import (
	"math"
	"time"

	"github.com/Clever/oplog-replay/ratecontroller"
	"labix.org/v2/mgo/bson"
)

type relativeRateController struct {
	speedMultiplier float64
	logStartTime    int
	startTime       time.Time
}

func (controller *relativeRateController) WaitTime(op map[string]interface{}) time.Duration {
	eventTime := int((op["ts"].(bson.MongoTimestamp)) >> 32)
	if controller.logStartTime == 0 {
		controller.logStartTime = eventTime
	}

	relativeEventTime := float64(eventTime - controller.logStartTime)
	// Scale the event time by the speed multipler
	scaledEventTime := relativeEventTime / controller.speedMultiplier
	timeElapsed := time.Now().Sub(controller.startTime).Seconds()

	// Convert to ms to avoid rounding issues
	msToWait := math.Max(scaledEventTime-timeElapsed, 0.0) * 1000
	return time.Duration(msToWait) * time.Millisecond
}

// New returns a rate controller that the plays the oplog at a speed that's a
// multiple of the original oplog speed.
func New(speed float64) ratecontroller.Controller {
	if speed == -1 || speed == 0 {
		speed = math.Inf(1)
	}
	return &relativeRateController{speedMultiplier: speed, startTime: time.Now()}
}
