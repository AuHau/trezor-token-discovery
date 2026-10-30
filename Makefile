BIN := bin/tokendiscovery
PKG := ./cmd/tokendiscovery

.PHONY: all build test test-race test-integration golden fuzz lint fmt vet tidy clean measure measure-curve run-serve

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

# Rewrite decoder golden files. Only legitimate when the decoding semantics
# changed on purpose — in which case bump evm.ParserVersion too.
golden:
	go test ./internal/chains/evm -run Golden -update

# The decoder parses untrusted provider output, so fuzz it.
fuzz:
	go test ./internal/chains/evm -run=Fuzz -fuzz=FuzzDecode -fuzztime=60s

# Integration tests against a real endpoint (skipped without RPC_URL).
test-integration:
	go test ./... -run Integration -count=1 -v

# Phase 1 measurement tool. Requires RPC_URL; see docs/measurements.md.
measure:
	go run ./tools/measure -windows 10 -window-size 200

measure-curve:
	go run ./tools/measure -curve 12000 -curve-chunk 300

run-serve: build
	$(BIN) serve

clean:
	rm -rf bin data
