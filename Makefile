BIN := bin/tokendiscovery
PKG := ./cmd/tokendiscovery

.PHONY: all build test lint fmt vet tidy clean measure run-serve

all: fmt vet test build

build:
	@mkdir -p bin
	go build -o $(BIN) $(PKG)

test:
	go test ./...

# Race detector on the packages that will carry concurrency (indexer, API).
test-race:
	go test -race ./...

fmt:
	gofmt -l -w .

vet:
	go vet ./...

tidy:
	go mod tidy

# Phase 1 measurement tool. Requires RPC_URL; see docs/measurements.md.
measure:
	go run ./tools/measure -windows 10 -window-size 200

measure-curve:
	go run ./tools/measure -curve 12000 -curve-chunk 300

run-serve: build
	$(BIN) serve

clean:
	rm -rf bin data
