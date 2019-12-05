.PHONY: all dataset ingestion query
all: dataset ingestion query

dataset: ftsb_generate_data ftsb_generate_queries

ingestion: ftsb_load_redisearch

query: ftsb_run_queries_redisearch

%: $(wildcard ./cmd/$@/*.go)
	go build -o bin/$@ ./cmd/$@
	go install ./cmd/$@

test:
	go test -v -race -coverprofile=coverage.txt -covermode=atomic ./...

collect-detach:
	docker-compose -f contrib/docker-compose.yml up --force-recreate -d
	sleep 5
	open http://localhost:3000/d/1/redisearch?orgId=1&refresh=5s

collect:
	docker-compose -f contrib/docker-compose.yml up --force-recreate

collect-stop:
	docker-compose -f contrib/docker-compose.yml down