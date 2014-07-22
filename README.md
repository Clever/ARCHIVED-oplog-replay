Oplog Replay
============

A tool to replay MongoDB oplogs at a multiple of the original speed. Useful for stress testing databases using real world data.

Usage
-----

First build Oplog Replay with:

`go build`

Then run it and pipe in a bson file containing the oplog you want to replay:

`./oplog-replay < oplog.rs.bson`

-----

You can also specify the following flags:

flag      | default     | description
:-------: | :---------: | :---------:
`--speed` | `1`         | Multiplier for playback speed.
`--host`  | `localhost` | Host that the oplog will be replayed against.


Getting an Oplog
----------------

You can get an oplog dump by specifying the collection directly:

`mongodump --db local --collection oplog.rs`

A `--query` flag can be specified to get only certain oplog entries.
