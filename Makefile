SHELL := /bin/bash
PKGS := \
github.com/Clever/oplog-replay/replay \
github.com/Clever/oplog-replay/bson \
github.com/Clever/oplog-replay/cmd/oplog-replay

.PHONY: test golint README

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
