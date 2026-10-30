.PHONY: all build test test-race lint format clean run-tracker

BINARY_NAME=p2p
BUILD_DIR=bin

all: lint test build

build:
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/p2p

test:
	go test -v ./...

test-race:
	go test -v -race -covermode=atomic -coverprofile=coverage.out ./...

lint:
	go vet ./...
	@test -z "$$(gofmt -s -l . | tee /dev/stderr)" || (echo "Please run 'make format'" && exit 1)

format:
	gofmt -s -w .

clean:
	rm -rf $(BUILD_DIR) dist coverage.out coverage.html tmp/

run-tracker: build
	./$(BUILD_DIR)/$(BINARY_NAME) tracker --port 8080
