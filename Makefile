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
all: get test ftsb_redisearch

# Build-time GIT variables
ifeq ($(GIT_SHA),)
GIT_SHA:=$(shell git rev-parse HEAD)
endif

ifeq ($(GIT_DIRTY),)
GIT_DIRTY:=$(shell git diff --no-ext-diff 2> /dev/null | wc -l)
endif

LDFLAGS = "-X 'main.GitSHA1=$(GIT_SHA)' -X 'main.GitDirty=$(GIT_DIRTY)'"

fmt:
	$(GOFMT) ./...

ftsb_redisearch: test
	$(GOBUILD) \
		-ldflags=$(LDFLAGS) \
		-o bin/$@ ./cmd/$@

get:
	$(GOGET) ./...

test: get

release:
	$(GOGET) github.com/mitchellh/gox
	$(GOGET) github.com/tcnksm/ghr
	GO111MODULE=on gox  -osarch "linux/amd64 darwin/amd64" \
		-ldflags=$(LDFLAGS) \
		-output "${DISTDIR}/${BIN_NAME}_{{.OS}}_{{.Arch}}" ./cmd/ftsb_redisearch

publish: release
	@for f in $(shell ls ${DISTDIR}); \
	do \
	echo "copying ${DISTDIR}/$${f}"; \
	aws s3 cp ${DISTDIR}/$${f} s3://benchmarks.redislabs/${MODULE}/tools/${BIN_NAME}/$${f} --acl public-read; \
	done
