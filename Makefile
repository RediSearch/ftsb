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
OS_ARCHs = linux/amd64 linux/arm64 linux/arm windows/amd64 darwin/amd64 darwin/arm64

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
	@mkdir -p ${DISTDIR}
	@for pair in $(OS_ARCHs); do \
		os=$${pair%/*}; arch=$${pair#*/}; \
		ext=""; \
		if [ "$$os" = "windows" ]; then ext=".exe"; fi; \
		out="${DISTDIR}/${BIN_NAME}_$${os}_$${arch}$${ext}"; \
		echo "==> building $$os/$$arch -> $$out"; \
		GOOS=$$os GOARCH=$$arch CGO_ENABLED=0 $(GOBUILD) \
			-ldflags=$(LDFLAGS) \
			-o $$out ./cmd/ftsb_redisearch || exit 1; \
	done

publish: release
	@for f in $(shell ls ${DISTDIR}); \
	do \
	echo "copying ${DISTDIR}/$${f}"; \
	aws s3 cp ${DISTDIR}/$${f} s3://benchmarks.redislabs/redisearch/tools/ftsb/$${f} --acl public-read; \
	done
