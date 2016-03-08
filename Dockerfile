# oplog-replay worker
FROM ubuntu:14.04
RUN apt-get -y update && \
    apt-get install -y wget build-essential && \
    apt-get -y update && \
    apt-get install -y -q curl

# Mongo
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 7F0CEB10
RUN echo 'deb http://downloads-distro.mongodb.org/repo/ubuntu-upstart dist 10gen' | sudo tee /etc/apt/sources.list.d/mongodb.list
RUN apt-get -y update && apt-get install -y mongodb-org

# Gearcmd
RUN curl -L https://github.com/Clever/gearcmd/releases/download/v0.3.8/gearcmd-v0.3.8-linux-amd64.tar.gz | tar xz -C /usr/local/bin --strip-components 1

COPY bin/oplog-replay /usr/local/bin/oplog-replay
CMD ["gearcmd", "--name", "oplog-replay", "--cmd", "/usr/local/bin/oplog-replay", "--cmdtimeout", "8h"]
