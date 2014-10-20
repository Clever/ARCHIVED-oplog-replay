package replay

import (
	"fmt"
	"io"
	"log"

	bsonScanner "github.com/Clever/oplog-replay/bson"
	"github.com/Clever/oplog-replay/ratecontroller"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"

	"time"
)

// ParseBSON parses the bson from the Reader interface. It writes each operation to the opChannel.
// If there are any errors it closes the opChannel and returns immediately.
func parseBSON(r io.Reader, opChannel chan map[string]interface{}) error {
	defer close(opChannel)

	scanner := bsonScanner.New(r)
	for scanner.Scan() {
		op := map[string]interface{}{}
		if err := bson.Unmarshal(scanner.Bytes(), &op); err != nil {
			return err
		}
		opChannel <- op
	}
	if scanner.Err() != nil {
		return scanner.Err()
	}
	return nil
}

// getAllElementsCurrentlyInChannel returns a slice of all the elements that can be retreived from the
// channel without blocking. It also returns a boolean that's true if the channel has been closed.
func getAllElementsCurrentlyInChannel(channel chan map[string]interface{}) ([]interface{}, bool) {
	var elements []interface{}
	// In a loop grab as many elements as you can before you would block (the default case)
	for {
		select {
		case elem, channelOpen := <-channel:
			if !channelOpen {
				return elements, true
			}
			elements = append(elements, elem)
		default:
			// In this case there are no more elements in the channel and it hasn't been closed
			return elements, false
		}
	}
}

func oplogReplay(ops chan map[string]interface{}, applyOps func([]interface{}) error, controller ratecontroller.Controller) error {
	// The choice of 20 for the maximum number of operations to apply at once is fairly arbitrary
	timedOps := make(chan map[string]interface{}, 20)
	// Run a goroutine that applies the ops. If there are any errors in application this returns immediately.
	// It sets the timedOpsReturnVal channel with the error response.
	timedOpsReturnVal := make(chan error)
	go func() {
		// Repeatedly grab as many elements as possible from the channel. If there aren't any then sleep,
		// otherwise apply them.
		for {
			opsToApply, closed := getAllElementsCurrentlyInChannel(timedOps)
			if len(opsToApply) > 0 {
				if err := applyOps(opsToApply); err != nil {
					timedOpsReturnVal <- err
					return
				}
			}
			if closed {
				break
			}
			if len(opsToApply) == 0 {
				time.Sleep(time.Duration(1) * time.Millisecond)
			}
		}
		timedOpsReturnVal <- nil
	}()
	for op := range ops {
		if op["ns"] == "" {
			// Can't apply ops without a db name
			continue
		}
		time.Sleep(controller.WaitTime(op))
		timedOps <- op
	}
	close(timedOps)
	returnVal := <-timedOpsReturnVal
	close(timedOpsReturnVal)
	return returnVal
}

// getApplyOpsFunc returns the applyOps function. It's separated out for unit testing
func getApplyOpsFunc(session *mgo.Session) func([]interface{}) error {
	return func(ops []interface{}) error {
		var result map[string]interface{}
		if err := session.Run(bson.M{"applyOps": ops, "alwaysUpsert": false}, &result); err != nil {
			return err
		}
		// We have to inspect the response from session.Run to determine if the oplog operation
		// was applied correctly.
		resultsArray, ok := result["results"].([]interface{})
		if !ok {
			return fmt.Errorf("Failed to cast %v as []interfaces{}", result["results"])
		}
		for index, opResult := range resultsArray {
			boolResult, ok := opResult.(bool)
			if !ok {
				return fmt.Errorf("Failed to cast %v as bool", opResult)
			}
			if !boolResult {
				return fmt.Errorf("Operation %v failed", ops[index])
			}
		}
		numApplied, ok := result["applied"].(int)
		if !ok {
			return fmt.Errorf("Failed to cast applied %s as int", numApplied)
		}
		if numApplied != len(ops) {
			return fmt.Errorf("Operations applied %s does not match operations sent %s", numApplied, len(ops))
		}
		return nil
	}
}

// ReplayOplog replays an oplog onto the specified host. If there are any errors this function
// terminates and returns the error immediately.
// ReplayOplog replays an oplog onto the specified host
func ReplayOplog(r io.Reader, controller ratecontroller.Controller, host string) error {
	log.Println("Parsing BSON...")
	opChannel := make(chan map[string]interface{})
	parseBSONReturnVal := make(chan error)
	go func() {
		parseBSONReturnVal <- parseBSON(r, opChannel)
	}()
	session, err := mgo.Dial(host)
	if err != nil {
		return err
	}
	defer session.Close()

	applyOps := getApplyOpsFunc(session)
	log.Println("Begin replaying...")

	if err := oplogReplay(opChannel, applyOps, controller); err != nil {
		return err
	}
	retVal := <-parseBSONReturnVal
	close(parseBSONReturnVal)
	return retVal
}
