ENV = $(shell go env GOPATH)
GO_VERSION = $(shell go version)

# Look for versions prior to 1.10 which have a different fmt output
# and don't lint with gofmt against them.
ifneq (,$(findstring go version go1.8, $(GO_VERSION)))
	FMT=
else ifneq (,$(findstring go version go1.9, $(GO_VERSION)))
	FMT=
else
    FMT=--enable gofmt
endif

lint:
	gofmt -s -w .
	golangci-lint run -c .golangci-lint.yml $(FMT) .

.PHONY: lint