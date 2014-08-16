Oplog Replay
============

A library, binary, and worker for replaying MongoDB oplogs at a multiple of the original speed. Useful for stress testing databases using real world data.

Usage as a binary
-----------------

First build Oplog Replay and put it on your GOPATH with:

`go get github.com/Clever/oplog-replay/cmd`

Then run it and pipe in a bson file containing the oplog you want to replay:

`oplog-replay < oplog.rs.bson`

-----

You can also specify the following flags:

flag       | default     | description
:-------:  | :---------: | :---------:
`--speed`  | `1`         | Multiplier for playback speed.
`--host`   | `localhost` | Host that the oplog will be replayed against.
`--s3path` | ''          | Read the bson file from the s3path instead of stdin.
`--retries | '0'         | Retries the replay until it succeeds or this many times.

Usage as a library
------------------

Include it in your code: include "github.com/Clever/oplog-replay/replay"

And call it as follows: replay.ReplayOplog(r io.Reader, float64 speed, host string)


Getting an Oplog
----------------

You can get an oplog dump by specifying the collection directly:

`mongodump --db local --collection oplog.rs`

A `--query` flag can be specified to get only certain oplog entries.
