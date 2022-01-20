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
		--dsn1 'root:@tcp(127.0.0.1:4000)/?time_zone=UTC' \
		--dsn2 'root:@tcp(127.0.0.1:3306)/?time_zone=UTC' --count 200 --debug

test-syntax: build
	@bin/sqlgen check-syntax --dsn 'root:@tcp(127.0.0.1:4000)/?time_zone=UTC' --count 200 --debug

gen: build
	@bin/sqlgen print --count 200
