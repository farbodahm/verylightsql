.PHONY: build run test test-specific clean

# Binary name
BINARY=verylightsql
DB_FILE=vlsql.db

# Build the binary
build:
	go build .

# Run the application 
run: build
	./$(BINARY)

# Run all integration tests
test: build
	go test -v -tags=integration ./...

# Run a specific test 
# Usage: make test-specific TEST=TestName
test-specific: build
	@if [ -z "$(TEST)" ]; then \
		echo "Error: TEST variable is required. Usage: make test-specific TEST=TestName"; \
		exit 1; \
	fi
	go test -timeout 30s -run ^$(TEST)$$ github.com/farbodahm/verylightsql -tags=integration

# Clean build artifacts
clean:
	rm -f $(BINARY) $(DB_FILE)
