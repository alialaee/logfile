.PHONY: all test lint

all: lint test

test:
	@echo "Running tests..."
	@go test -v ./...

lint:
	@echo "Running golangci-lint..."
	@golangci-lint run
