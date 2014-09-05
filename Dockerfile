# oplog-replay worker
FROM ubuntu:14.04
RUN apt-get update
RUN apt-get install -y wget build-essential

# Golang
RUN apt-get install -y git golang bzr mercurial bash
RUN GOPATH=/etc/go go get launchpad.net/godeb
RUN apt-get remove -y golang golang-go golang-doc golang-src
RUN /etc/go/bin/godeb install 1.2.1

# Oplog replay
RUN mkdir -p /etc/go/src /github.com/Clever/oplog-replay
ADD . /etc/go/src/github.com/Clever/oplog-replay
RUN GOPATH=/etc/go go get github.com/Clever/oplog-replay/...
RUN GOPATH=/etc/go go build -o /usr/local/bin/oplogreplay github.com/Clever/oplog-replay/cmd/oplog-replay

# Taskwrapper
RUN mkdir -p /etc/go/src /taskwrapper
RUN GOPATH=/etc/go go get github.com/Clever/baseworker-go/cmd/taskwrapper
RUN GOPATH=/etc/go go build -o /usr/local/bin/taskwrapper github.com/Clever/baseworker-go/cmd/taskwrapper

CMD ["/etc/go/src/github.com/Clever/oplog-replay/run_as_worker.sh"]
