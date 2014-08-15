package main

import (
	"flag"
	"os"

	"github.com/Clever/oplog-replay/ratecontroller"
	"github.com/Clever/oplog-replay/replay"
)

func main() {
	host := flag.String("host", "localhost", "Mongo host to playback onto.")
	ratetype := flag.String("type", "fixed", "Type of rate limiting. Valid options are 'fixed' and 'relative'. See 'speed' for details on these types,")
	speed := flag.Float64("speed", 1, "Sets the speed of the replay. For 'fixed' type replays this indicates the operations per second. For 'relative' type operations this indicates the speed relative to the initial oplog replay.")
	flag.Parse()

	controller, err := ratecontroller.New(*ratetype, *speed)
	if err != nil {
		panic(err)
	}
	if err := replay.ReplayOplog(os.Stdin, controller, *host); err != nil {
		panic(err)
	}
}
