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

// FailedOperationError means that an operation failed to apply.
type FailedOperationError struct {
	op  map[string]interface{}
	msg string
}

func (e *FailedOperationError) Error() string { return e.msg }

// NewFailedOperationError creates and returns a FailedOperationsError for the given op.
func NewFailedOperationError(op map[string]interface{}) *FailedOperationError {
	return &FailedOperationError{
		op:  op,
		msg: fmt.Sprintf("Operation %v failed", op),
	}
}

// ParseBSON parses the bson from the Reader interface. It returns a channel that the caller can use
// to retrieve the parsed BSON ops, and a channel for parse errors.
func parseBSON(done <-chan struct{}, r io.Reader) (<-chan map[string]interface{}, <-chan error) {
	c := make(chan map[string]interface{})
	errc := make(chan error, 1)

	go func() {
		defer close(c)
		scanner := bsonScanner.New(r)
	scan:
		for scanner.Scan() {
			op := map[string]interface{}{}
			if err := bson.Unmarshal(scanner.Bytes(), &op); err != nil {
				errc <- err
				return
			}
			select {
			case c <- op:
			case <-done:
				break scan
			}
		}
		if err := scanner.Err(); err != nil {
			fmt.Println(err)
			errc <- err
		}
		errc <- nil
	}()
	return c, errc
}

// controlRate takes operations on an input channel puts them into the returned output
// channel at a rate dictated by the passed in rate controller.
func controlRate(done <-chan struct{}, ops <-chan map[string]interface{},
	controller ratecontroller.Controller) <-chan map[string]interface{} {
	// The choice of 20 for the maximum number of operations to apply at once is fairly arbitrary
	c := make(chan map[string]interface{}, 20)

	go func() {
		defer close(c)
		for op := range ops {
			if op["ns"] == "" {
				continue
			}
			time.Sleep(controller.WaitTime(op))
			select {
			case c <- op:
			case <-done:
			}
		}
	}()
	return c
}

// batchOps takes an input buffered channel and returns a channel which will contain batched
// ops.  The maximum batch size is the size of the buffered input channel.
func batchOps(done <-chan struct{}, ops <-chan map[string]interface{}) <-chan []interface{} {
	c := make(chan []interface{})

	go func() {
		defer close(c)
		// In a loop grab as many elements as you can before you would block (the default case)
		// Only place non-empty batches into the output channel.
		elements := make([]interface{}, 0)

		// Send the current list of elements as a batch, unless it's empty. Returns whether or not a batch was sent.
		sendElements := func() bool {
			if len(elements) == 0 {
				return false
			}
			select {
			case c <- elements:
				elements = make([]interface{}, 0)
			case <-done:
			}
			return true
		}

		for {
			select {
			case elem, channelOpen := <-ops:
				if !channelOpen {
					sendElements()
					return
				}
				elements = append(elements, elem)
			default:
				if !sendElements() {
					// stop from busy-wait eating all cpu.
					time.Sleep(1 * time.Millisecond)
				}
			}
		}
	}()
	return c
}

// oplogReplay takes in a channel of batched operations and applys them using the
// supplied function.  Returns an error if the apply operation fails.
func oplogReplay(batches <-chan []interface{}, applyOps func([]interface{}) error) error {
	for batch := range batches {
		if err := applyOps(batch); err != nil {
			return err
		}
	}
	return nil
}

// getApplyOpsFunc returns the applyOps function. It's separated out for unit testing
func getApplyOpsFunc(session *mgo.Session, alwaysUpsert bool) func([]interface{}) error {
	return func(ops []interface{}) error {
		var result map[string]interface{}
		if err := session.Run(bson.D{{"applyOps", ops}, {"alwaysUpsert", alwaysUpsert}}, &result); err != nil {
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
				failedOp := ops[index].(map[string]interface{})
				return NewFailedOperationError(failedOp)
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
func ReplayOplog(r io.Reader, controller ratecontroller.Controller, alwaysUpsert bool, host string) error {
	done := make(chan struct{})
	defer close(done)

	session, err := mgo.Dial(host)
	if err != nil {
		return err
	}
	defer session.Close()

	log.Println("Parsing BSON...")
	ops, parseErrors := parseBSON(done, r)
	timedOps := controlRate(done, ops, controller)
	batchedOps := batchOps(done, timedOps)

	applyOps := getApplyOpsFunc(session, alwaysUpsert)
	log.Println("Begin replaying...")

	if err := oplogReplay(batchedOps, applyOps); err != nil {
		return err
	}
	if err := <-parseErrors; err != nil {
		return err
	}
	return nil
}
