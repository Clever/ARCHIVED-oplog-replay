SHELL := /bin/bash
PKGS := \
github.com/Clever/oplog-replay/replay \
github.com/Clever/oplog-replay/bson \
github.com/Clever/oplog-replay/cmd/oplog-replay \
github.com/Clever/oplog-replay/ratecontroller \
github.com/Clever/oplog-replay/ratecontroller/fixed \
github.com/Clever/oplog-replay/ratecontroller/relative

.PHONY: test golint README

GOVERSION := $(shell go version | grep 1.5)
ifeq "$(GOVERSION)" ""
  $(error must be running Go version 1.5)
endif

export GO15VENDOREXPERIMENT = 1

all: build

build:
	go build -o bin/oplog-replay "github.com/Clever/oplog-replay/cmd/oplog-replay"

clean:
	rm bin/*

test: $(PKGS)

golint:
	@go get github.com/golang/lint/golint

$(PKGS): golint README
	@go get -d -t $@
	@gofmt -w=true $(GOPATH)/src/$@/*.go
ifneq ($(NOLINT),1)
	@echo "LINTING..."
	@PATH=$(PATH):$(GOPATH)/bin golint $(GOPATH)/src/$@/*.go
	@echo ""
endif
ifeq ($(COVERAGE),1)
	@go test -cover -coverprofile=$(GOPATH)/src/$@/c.out $@ -test.v
	@go tool cover -html=$(GOPATH)/src/$@/c.out
else
	@echo "TESTING..."
	@go test $@ -test.v
	@echo ""
endif


SHELL := /bin/bash
PKGS := $(shell go list ./... | grep -v /vendor)
GODEP := $(GOPATH)/bin/godep

$(GODEP):
	go get -u github.com/tools/godep

vendor: $(GODEP)
	$(GODEP) save $(PKGS)
	find vendor/ -path '*/vendor' -type d | xargs -IX rm -r X # remove any nested vendor directories
