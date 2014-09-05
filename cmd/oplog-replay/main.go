package main

import (
	"flag"
	"fmt"

	"github.com/Clever/oplog-replay/ratecontroller"
	"github.com/Clever/oplog-replay/ratecontroller/fixed"
	"github.com/Clever/oplog-replay/ratecontroller/relative"
	"github.com/Clever/oplog-replay/replay"
	"github.com/Clever/pathio"
)

func main() {
	host := flag.String("host", "localhost", "Mongo host to playback onto.")
	ratetype := flag.String("type", "fixed", "Type of rate limiting. Valid options are 'fixed' and 'relative'. See 'speed' for details on these types,")
	speed := flag.Float64("speed", 1, "Sets the speed of the replay. For 'fixed' type replays this indicates the operations per second. For 'relative' type operations this indicates the speed relative to the initial oplog replay.")
	path := flag.String("path", "/dev/stdin", "Oplog file to replay")
	flag.Parse()

	controller, err := getControllerFromTypeAndSpeed(*ratetype, *speed)
	if err != nil {
		panic(err)
	}
	input, err := pathio.Reader(*path)
	if err != nil {
		panic(err)
	}
	if err := replay.ReplayOplog(input, controller, *host); err != nil {
		panic(err)
	}
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
