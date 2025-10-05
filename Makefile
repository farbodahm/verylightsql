.PHONY: build run test test-specific db-init clean

BINARY=verylightsql
DB_FILE=vlsql.db
NUM_RECORDS=500

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

# Initialize database with sample records
# Usage: make db-init [NUM_RECORDS=100]
db-init: build
	@echo "Initializing database with $(NUM_RECORDS) records..."
	@rm -f $(DB_FILE)
	@(for i in $$(seq 1 $(NUM_RECORDS)); do \
		echo "insert $$i user$$i person$$i@example.com"; \
	done; \
	echo ".exit") | ./$(BINARY) > /dev/null
	@echo "Database initialized with $(NUM_RECORDS) records in $(DB_FILE)"

# Clean build artifacts
clean:
	rm -f $(BINARY) $(DB_FILE)
