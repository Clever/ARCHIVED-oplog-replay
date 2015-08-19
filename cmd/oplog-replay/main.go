package main

import (
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/Clever/oplog-replay/ratecontroller"
	"github.com/Clever/oplog-replay/ratecontroller/fixed"
	"github.com/Clever/oplog-replay/ratecontroller/relative"
	"github.com/Clever/oplog-replay/replay"
	"github.com/Clever/pathio"
	"github.com/cenkalti/backoff"
)

func main() {
	host := flag.String("host", "localhost", "Mongo host to playback onto.")
	ratetype := flag.String("type", "fixed", "Type of rate limiting. Valid options are 'fixed' and 'relative'. See 'speed' for details on these types,")
	speed := flag.Float64("speed", 1, "Sets the speed of the replay. For 'fixed' type replays this indicates the operations per second. For 'relative' type operations this indicates the speed relative to the initial oplog replay.")
	path := flag.String("path", "/dev/stdin", "Oplog file to replay")
	// See https://github.com/mongodb/docs/commit/238d6755a74c3c978cc272d318283f726379a43c for more details on the behavior of upsert
	alwaysUpsert := flag.Bool("alwaysUpsert", false, "Convert all updates to upserts. Converting all updates to upserts prevents errors when replaying oplog dumps that have updates to documents followed by deletes to those same documents. Note that this flag is only applicable in Mongo version 2.6 and above.")
	flag.Parse()

	controller, err := getControllerFromTypeAndSpeed(*ratetype, *speed)
	if err != nil {
		panic(err)
	}
	input, err := readerWithRetry(*path)
	if err != nil {
		panic(err)
	}
	if err := replay.ReplayOplog(input, controller, *alwaysUpsert, *host); err != nil {
		panic(err)
	}
}

// readerWithTimeout gets a reader from the path, error if timeout.
func readerWithTimeout(path string) (io.Reader, error) {
	var reader io.Reader
	var err error

	c := make(chan bool, 1)
	go func() {
		reader, err = pathio.Reader(path)
		c <- true
	}()

	select {
	case <-c:
		return reader, err
	case <-time.After(5 * time.Minutes):
		return reader, fmt.Errorf("pathio timed-out")
	}
}

// readerWithRetry gets a reader from the path, retrying if necessary.
func readerWithRetry(path string) (io.Reader, error) {
	backoffObj := backoff.ExponentialBackOff{
		InitialInterval:     5 * time.Second,
		RandomizationFactor: backoff.DefaultRandomizationFactor,
		Multiplier:          2,
		MaxInterval:         30 * time.Second,
		MaxElapsedTime:      2 * time.Minute,
		Clock:               backoff.SystemClock,
	}

	var reader io.Reader
	operation := func() error {
		var err error
		reader, err = readerWithTimeout(path)
		return err
	}
	if err := backoff.Retry(operation, &backoffObj); err != nil {
		return nil, err
	}
	return reader, nil

}

func getControllerFromTypeAndSpeed(ratetype string, speed float64) (ratecontroller.Controller, error) {
	if ratetype == "fixed" {
		return fixed.New(speed), nil
	} else if ratetype == "relative" {
		return relative.New(speed), nil
	} else {
		return nil, fmt.Errorf("Unknown type: " + ratetype)
	}
}
