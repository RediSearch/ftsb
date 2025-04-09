# Go parameters
GOCMD=GO111MODULE=on go
GOBUILD=$(GOCMD) build
GOINSTALL=$(GOCMD) install
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOFMT=$(GOCMD) fmt
BIN_NAME=ftsb_redisearch
MODULE=ftsb_redisearch
DISTDIR = ./dist

.PHONY: ftsb_redisearch
all: get ftsb_redisearch integration-test

# Build-time GIT variables
ifeq ($(GIT_SHA),)
GIT_SHA:=$(shell git rev-parse HEAD)
endif

ifeq ($(GIT_DIRTY),)
GIT_DIRTY:=$(shell git diff --no-ext-diff 2> /dev/null | wc -l)
endif

LDFLAGS = "-X 'main.GitSHA1=$(GIT_SHA)' -X 'main.GitDirty=$(GIT_DIRTY)'"
OS_ARCHs = "linux/amd64 linux/arm64 linux/arm windows/amd64 darwin/amd64 darwin/arm"

build:
	$(GOBUILD) \
        -ldflags=$(LDFLAGS) ./cmd/ftsb_redisearch
fmt:
	$(GOFMT) ./...

ftsb_redisearch:
	$(GOBUILD) \
		-ldflags=$(LDFLAGS) \
		-o bin/ftsb_redisearch ./cmd/ftsb_redisearch

get:
	$(GOGET) ./...

integration-test: get ftsb_redisearch
	$(GOTEST) -v $(shell go list ./... | grep -v '/cmd/')

release:
	$(GOGET) github.com/mitchellh/gox
	$(GOGET) github.com/tcnksm/ghr
	GO111MODULE=on gox  -osarch ${OS_ARCHs} \
		-ldflags=$(LDFLAGS) \
		-output "${DISTDIR}/${BIN_NAME}_{{.OS}}_{{.Arch}}" ./cmd/ftsb_redisearch

publish: release
	@for f in $(shell ls ${DISTDIR}); \
	do \
	echo "copying ${DISTDIR}/$${f}"; \
	aws s3 cp ${DISTDIR}/$${f} s3://benchmarks.redislabs/redisearch/tools/ftsb/$${f} --acl public-read; \
	done
