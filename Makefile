all: build

build: fmt
	@echo "Building binary..."
	@go build -o bin/sqlgen

fmt:
	@echo "go fmt..."
	@go fmt ./...

test: build
	@go test ./...

abtest: build
	@bin/sqlgen abtest \
		--target-policy "$(SQLGEN_TARGET_POLICY)" --count 200 --debug --seed=1652779915

test-syntax: build
	@bin/sqlgen check-syntax --target-policy "$(SQLGEN_TARGET_POLICY)" --count 200 --debug

gen: build
	@bin/sqlgen print --count 200
