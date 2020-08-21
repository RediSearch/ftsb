# Go parameters
GOCMD=GO111MODULE=on go
GOBUILD=$(GOCMD) build
GOINSTALL=$(GOCMD) install
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOFMT=$(GOCMD) fmt

# DOCKER
DOCKER_APP_NAME=ftsb
DOCKER_ORG=redisbench
DOCKER_REPO:=${DOCKER_ORG}/${DOCKER_APP_NAME}
#DOCKER_TAG:=$(shell git log -1 --pretty=format:"%h")
DOCKER_TAG=edge
DOCKER_IMG:="$(DOCKER_REPO):$(DOCKER_TAG)"
DOCKER_LATEST:="${DOCKER_REPO}:latest"

.PHONY: all benchmark
all: get test benchmark

benchmark: ftsb_redisearch

release:
	$(GOGET) github.com/mitchellh/gox
	$(GOGET) github.com/tcnksm/ghr
	GO111MODULE=on gox  -osarch "linux/amd64 darwin/amd64" -output "dist/ftsb_redisearch_{{.OS}}_{{.Arch}}" ./cmd/ftsb_redisearch
	#ghr -t $GITHUB_TOKEN  --replace `git describe --tags` dist/

fmt:
	$(GOFMT) ./...

ftsb_redisearch:
	$(GOBUILD) -o bin/$@ ./cmd/$@
	$(GOINSTALL) ./cmd/$@

get:
	$(GOGET) ./...

test: get
	$(GOTEST) -race -coverprofile=coverage.txt -covermode=atomic ./...

# DOCKER TASKS
# Build the container
docker-build:
	docker build -t $(DOCKER_APP_NAME):latest -f  Dockerfile .

# Build the container without caching
docker-build-nc:
	docker build --no-cache -t $(DOCKER_APP_NAME):latest -f  Dockerfile .

# Make a release by building and publishing the `{version}` ans `latest` tagged containers to ECR
docker-release: docker-build-nc docker-publish

# Docker publish
docker-publish: docker-publish-latest docker-publish-version ## Publish the `{version}` ans `latest` tagged containers to ECR

docker-repo-login: ## login to DockerHub with credentials found in env
	docker login -u ${DOCKER_USERNAME} -p ${DOCKER_PASSWORD}

docker-publish-latest: docker-tag-latest ## Publish the `latest` taged container to ECR
	@echo 'publish latest to $(DOCKER_REPO)'
	docker push $(DOCKER_LATEST)

docker-publish-version: docker-tag-version ## Publish the `{version}` taged container to ECR
	@echo 'publish $(DOCKER_IMG) to $(DOCKER_REPO)'
	docker push $(DOCKER_IMG)

# Docker tagging
docker-tag: docker-tag-latest docker-tag-version ## Generate container tags for the `{version}` ans `latest` tags

docker-tag-latest: ## Generate container `{version}` tag
	@echo 'create tag latest'
	docker tag $(DOCKER_APP_NAME) $(DOCKER_LATEST)

docker-tag-version: ## Generate container `latest` tag
	@echo 'create tag $(DOCKER_APP_NAME) $(DOCKER_REPO):$(DOCKER_IMG)'
	docker tag $(DOCKER_APP_NAME) $(DOCKER_IMG)