#!/bin/bash
gearcmd --name oplog-replay --cmd /usr/local/bin/oplogreplay --host $GEARMAN_HOST --port $GEARMAN_PORT &
pid=$!
# When we get a SIGTERM kill the child process and call wait. Note that we need wait both here
# and the line below because of the semantics of the kill syscall. It seems that wait returns
# the next time the state of the child process changes. Receiving a SIGTERM qualifies as a change
# of state, so if we don't have a wait after `kill $pid` then we will complete the trap handler
# and our call to `wait` below will complete. By calling `wait` after we kill the subprocess we wait
# for the next process state change which is the actual process termination.
trap "kill $pid && wait" SIGTERM SIGINT
# Wait so that this script keeps running
wait
