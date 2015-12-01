Oplog Replay
============

A library and a binary for replaying MongoDB oplogs at a multiple of the original speed. Useful for stress testing databases using real world data.

Usage as a binary
-----------------

First build Oplog Replay and put it on your GOPATH with:

`go get github.com/Clever/oplog-replay/cmd`

Then run it and pipe in a bson file containing the oplog you want to replay:

`oplog-replay < oplog.rs.bson`

-----

You can also specify the following flags:

flag      | default     | description
:-------: | :---------: | :---------:
`--speed` | `1`         | Multiplier for playback speed.
`--host`  | `localhost` | Host that the oplog will be replayed against.
`--path`  | `/dev/stdin` | Oplog file to replay

Usage as a library
------------------

Include it in your code: include "github.com/Clever/oplog-replay/replay"

And call it as follows: replay.ReplayOplog(r io.Reader, float64 speed, host string)


Getting an Oplog
----------------

You can get an oplog dump by specifying the collection directly:

`mongodump --db local --collection oplog.rs`

A `--query` flag can be specified to get only certain oplog entries.

## Changing Dependencies

### New Packages

When adding a new package, you can simply use `make vendor` to update your imports.
This should bring in the new dependency that was previously undeclared.
The change should be reflected in [Godeps.json](Godeps/Godeps.json) as well as [vendor/](vendor/).

### Existing Packages

First ensure that you have your desired version of the package checked out in your `$GOPATH`.

When to change the version of an existing package, you will need to use the godep tool.
You must specify the package with the `update` command, if you use multiple subpackages of a repo you will need to specify all of them.
So if you use package github.com/Clever/foo/a and github.com/Clever/foo/b, you will need to specify both a and b, not just foo.

```
# depending on github.com/Clever/foo
godep update github.com/Clever/foo

# depending on github.com/Clever/foo/a and github.com/Clever/foo/b
godep update github.com/Clever/foo/a github.com/Clever/foo/b
```

