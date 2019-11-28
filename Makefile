.PHONY: all dataset ingestion query
all: dataset ingestion query

dataset: ftsb_generate_data ftsb_generate_queries

ingestion: ftsb_load_redisearch

query: ftsb_run_queries_redisearch


%: $(wildcard ./cmd/$@/*.go)
	go build -o bin/$@ ./cmd/$@
	go install ./cmd/$@
