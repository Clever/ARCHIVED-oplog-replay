package fixed

import (
	"math"
	"time"

	"github.com/Clever/oplog-replay/ratecontroller"
)

type fixedRateController struct {
	opsPerSecond    float64
	totalOpsSeen    int
	replayStartTime time.Time
}

func (controller *fixedRateController) WaitTime(op map[string]interface{}) time.Duration {
	elapsedTime := time.Now().Sub(controller.replayStartTime).Seconds()

	// Figure out when we should apply the operation by doing the math
	timeShouldApplyOp := float64(controller.totalOpsSeen) / controller.opsPerSecond
	// Note that we convert to milliseconds because otherwise we seem to run into rounding errors
	msToWait := math.Max(timeShouldApplyOp-elapsedTime, 0) * 1000
	controller.totalOpsSeen++
	return time.Duration(msToWait) * time.Millisecond
}

// New returns a rate controller that controls oplog entries at a rate of
// X per second
func New(operationsPerSecond float64) ratecontroller.Controller {
	return &fixedRateController{opsPerSecond: operationsPerSecond, replayStartTime: time.Now()}
}
