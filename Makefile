.PHONY: build run test tidy fmt vet clean

BIN := bin/octo-message-export-api
PKG := ./...

build:
	@mkdir -p bin
	go build -o $(BIN) ./cmd/octo-message-export-api

run: build
	./$(BIN)

test:
	go test $(PKG)

tidy:
	go mod tidy

fmt:
	go fmt $(PKG)

vet:
	go vet $(PKG)

clean:
	rm -rf bin
