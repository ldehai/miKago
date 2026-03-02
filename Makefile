GO := go
BINARY := mikago

.PHONY: build run test clean fmt vet

build:
	$(GO) build -o $(BINARY) ./cmd/mikago/

run: build
	./$(BINARY)

test:
	$(GO) test ./... -v

clean:
	rm -f $(BINARY)
	$(GO) clean

fmt:
	$(GO) fmt ./...

vet:
	$(GO) vet ./...
