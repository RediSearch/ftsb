# Go parameters
GOCMD=GO111MODULE=on go
GOBUILD=$(GOCMD) build
GOINSTALL=$(GOCMD) install
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOFMT=$(GOCMD) fmt

.PHONY: ftsb_redisearch
all: get test ftsb_redisearch

release:
	$(GOGET) github.com/mitchellh/gox
	$(GOGET) github.com/tcnksm/ghr
	GO111MODULE=on gox  -osarch "linux/amd64 darwin/amd64" -output "dist/ftsb_redisearch_{{.OS}}_{{.Arch}}" ./cmd/ftsb_redisearch

fmt:
	$(GOFMT) ./...

ftsb_redisearch: test
	$(GOBUILD) -o bin/$@ ./cmd/$@
	$(GOINSTALL) ./cmd/$@

get:
	$(GOGET) ./...

test: get

