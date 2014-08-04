package main

import (
	"flag"
	"os"

	"github.com/Clever/oplog-replay/replay"
)

func main() {
	speed := flag.Float64("speed", 1, "Sets multiplier for playback speed.")
	host := flag.String("host", "localhost", "Mongo host to playback onto.")
	flag.Parse()

	if err := replay.ReplayOplog(os.Stdin, *speed, *host); err != nil {
		panic(err)
	}
}
