#!/bin/bash
gearcmd --name oplog-replay --cmd /usr/local/bin/oplogreplay --host $GEARMAN_HOST --port $GEARMAN_PORT &
pid=$!
# When we get a SIGTERM, forward it to the child process and call wait. Note that we wait both in here
# and below (on line 10) because when bash gets a SIGTERM bash appears to cancel the currently running
# command, call the trap handler, and then resume the script on the line after the line it was previously
# running. That means that without waiting in the trap we could exit the script before gearcmd actually exits.
trap "kill $pid && wait" SIGTERM SIGINT
# Wait so that this script keeps running
wait
