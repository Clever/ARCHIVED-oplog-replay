#!/bin/bash
gearcmd --name oplog-replay --cmd /usr/local/bin/oplogreplay --host $GEARMAN_HOST --port $GEARMAN_PORT
