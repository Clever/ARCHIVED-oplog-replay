# oplog-replay worker
FROM ubuntu:14.04
RUN apt-get -y update && apt-get install -y wget build-essential
RUN apt-get -y update && apt-get install -y -q curl


# Golang
RUN apt-get -y update && apt-get install -y git golang bzr mercurial bash
RUN GOPATH=/etc/go go get launchpad.net/godeb
RUN apt-get remove -y golang golang-go golang-doc golang-src
RUN /etc/go/bin/godeb install 1.5

# Mongo
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 7F0CEB10
RUN echo 'deb http://downloads-distro.mongodb.org/repo/ubuntu-upstart dist 10gen' | sudo tee /etc/apt/sources.list.d/mongodb.list
RUN apt-get -y update && apt-get install -y mongodb-org

# Oplog replay
RUN mkdir -p /etc/go/src /github.com/Clever/oplog-replay
ADD . /etc/go/src/github.com/Clever/oplog-replay
RUN GOPATH=/etc/go go get github.com/Clever/oplog-replay/...
RUN GOPATH=/etc/go go build -o /usr/local/bin/oplogreplay github.com/Clever/oplog-replay/cmd/oplog-replay

# Gearcmd
RUN curl -L https://github.com/Clever/gearcmd/releases/download/v0.3.8/gearcmd-v0.3.8-linux-amd64.tar.gz | tar xz -C /usr/local/bin --strip-components 1

CMD ["gearcmd", "--name", "oplog-replay", "--cmd", "/usr/local/bin/oplogreplay", "--cmdtimeout", "8h"]
