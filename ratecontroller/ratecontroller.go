package ratecontroller

import "time"

// Controller is an interface that can be used to control the rate at which oplog entries are applied.
type Controller interface {
	// WaitTime takes in an oplog entry and returns how long until that operation should be applied.
	// Note that WaitTime should only be called once for each operation.
	WaitTime(op map[string]interface{}) time.Duration
}
