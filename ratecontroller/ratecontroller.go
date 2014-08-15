package ratecontroller

import (
	"fmt"
	"math"
	"time"

	"labix.org/v2/mgo/bson"
)

// Controller is an interface that can be used to control the rate of operations. Its interface
// consists of a single method 'ShouldApplyOp' which returns true if the operation should be
// applied at the current time. Consumers of this interface should ask whether an operation should
// be applied until this returns true. Once this returns true for a given operation consumers
// should not ask about that same operation again.
type Controller interface {
	ShouldApplyOp(op map[string]interface{}) bool
}

// New is a factory method that creates controllers
func New(ratetype string, speed float64) (Controller, error) {
	if ratetype == "fixed" {
		return newFixedRateController(speed), nil
	} else if ratetype == "relative" {
		return newRelativeRateController(speed), nil
	} else {
		return nil, fmt.Errorf("Unknown rate type: ", ratetype)
	}
}

type fixedRateController struct {
	opsPerSecond    float64
	totalOpsSeen    int
	replayStartTime time.Time
}

func (controller *fixedRateController) ShouldApplyOp(op map[string]interface{}) bool {
	if controller.replayStartTime.IsZero() {
		controller.replayStartTime = time.Now()
	}
	secondsSinceStart := time.Now().Sub(controller.replayStartTime).Seconds()
	// Figure out our current rate and let operations through if we're below where
	// we want to be.
	rateSoFar := 0
	if secondSinceStart > 0 {
		rateSoFar = float64(controller.totalOpsSeen) / secondsSinceStart
	}
	shouldApplyOp := rateSoFar < controller.opsPerSecond
	if shouldApplyOp {
		controller.totalOpsSeen++
	}
	return shouldApplyOp
}

// newFixedRateController returns a rate controller that controls oplog entries at a rate of
// X per second
func newFixedRateController(operationsPerSecond float64) Controller {
	return &fixedRateController{opsPerSecond: operationsPerSecond}
}

type relativeRateController struct {
	speedMultiplier float64
	logStartTime    int
	replayStartTime time.Time
}

func (controller *relativeRateController) ShouldApplyOp(op map[string]interface{}) bool {
	eventTime := int((op["ts"].(bson.MongoTimestamp)) >> 32)
	if controller.replayStartTime.IsZero() {
		controller.replayStartTime = time.Now()
		controller.logStartTime = eventTime
	}

	// Adjust the time elapsed time so far. We will compare this to the time from the log start
	// to determine if we should apply this operation.
	adjustedTimeElapsed := time.Now().Sub(controller.replayStartTime).Seconds() * controller.speedMultiplier
	eventTimeFromStart := eventTime - controller.logStartTime
	return adjustedTimeElapsed > float64(eventTimeFromStart)
}

// newRelativeRateController returns a rate controller that the plays the oplog at a speed that's a
// multiple of the original oplog speed.
func newRelativeRateController(speed float64) Controller {
	if speed == -1 || speed == 0 {
		speed = math.Inf(1)
	}
	return &relativeRateController{speedMultiplier: speed}
}
