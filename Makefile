.PHONY: all go-test py-test build cli help clean

all: go-test py-test build

go-test:
	go test ./...

py-test:
	pytest -q

build:
	go build -o ampybusctl ./cmd/ampybusctl

clean:
	rm -f ampybusctl

help:
	@echo "Available targets:"
	@echo "  all      - Run all tests and build CLI (default)"
	@echo "  go-test  - Run Go tests"
	@echo "  py-test  - Run Python tests"
	@echo "  build    - Build CLI tool"
	@echo "  clean    - Remove built binaries"
	@echo "  help     - Show this help message"

cli:
	go build -o ampybusctl ./cmd/ampybusctl

replayer:
	go build -o replayer ./examples/go/replayer

run-replayer: replayer
	./replayer

sub-bars: cli
	./ampybusctl sub --subject "ampy.prod.bars.v1.>"
