ENV = $(shell go env GOPATH)
GO_VERSION = $(shell go version)
GO111MODULE=on

# Look for versions prior to 1.10 which have a different fmt output
# and don't lint with gofmt against them.
ifneq (,$(findstring go version go1.8, $(GO_VERSION)))
	FMT=
else ifneq (,$(findstring go version go1.9, $(GO_VERSION)))
	FMT=
else
    FMT=--enable gofmt
endif

test_rule: # @HELP execute tests
	@echo "executing tests"
	GOTRACEBACK=all go test $(TESTARGS) -count=1 -timeout=40s -race ./test/...
	GOTRACEBACK=all go test $(TESTARGS) -count=1 -timeout=40s -tags batchtest -race ./test/...

lint: # @HELP lint files and format if possible
	@echo "executing linter"
	gofmt -s -w .
	GO111MODULE=on golangci-lint run -c .golangci-lint.yml $(FMT) ./...

dep-linter: # @HELP install the linter dependency
	curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | sh -s -- -b $(ENV)/bin $(GOLANG_CI_VERSION)

deps: # @HELP install dependencies
	@echo "getting dependencies"
	go get -t -d -v ./...

build: # @HELP build the packages
	sh $(PWD)/scripts/build.sh

fuzz:
	go test $(TESTARGS) -count=5 -timeout=5m ./fuzzy
	go test $(TESTARGS) -count=5 -timeout=5m -tags batchtest ./fuzzy

ci: # @HELP executes on CI
ci: deps test_rule fuzz dep-linter lint

all: deps test_rule fuzz lint

.PHONY: all build