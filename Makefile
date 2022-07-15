TAG := $(shell git describe --tags --always --dirty)

all: build

version:
	@echo $(TAG)

build:
	go test -cover ./...
	go build  -v  ./...

update:
	go get -u ./...

