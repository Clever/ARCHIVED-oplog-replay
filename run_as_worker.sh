#!/bin/bash
taskwrapper --name oplog-replay --cmd /usr/local/bin/oplogreplay --gearman-host $GEARMAN_HOST --gearman-port $GEARMAN_PORT
