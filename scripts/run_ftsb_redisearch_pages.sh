#!/bin/bash

DATASET="enwiki-latest-abstract1"
MAX_QUERIES=10000
WORKERS=8
PRINT_INTERVAL=10000

# flush the database
redis-cli flushall

# create the index
redis-cli ft.create idx1 SCHEMA \
  TITLE TEXT WEIGHT 5 \
  URL TEXT WEIGHT 5 \
  ABSTRACT TEXT WEIGHT 1

redis-cli config resetstat

if [ -f /tmp/ftsb_generate_data-$DATASET-redisearch.gz ]; then
  cat /tmp/ftsb_generate_data-$DATASET-redisearch.gz |
    gunzip |
    ftsb_load_redisearch -workers $WORKERS -reporting-period 1s \
      -batch-size 1000 -pipeline 100
else
  echo "dataset file not found at /tmp/ftsb_generate_data-$DATASET-redisearch.gz"
fi

redis-cli info commandstats

echo "Benchmarking query execution performance"
for queryName in "simple-1word-query" "2word-union-query" "2word-intersection-query" "simple-1word-spellcheck"; do
  echo "Benchmarking query: $queryName"
  redis-cli config resetstat

  if [ -f /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0.gz ]; then
    cat /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0.gz |
      gunzip >/tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0

    ftsb_run_queries_redisearch \
      -file /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0 \
      -max-queries $MAX_QUERIES -workers $WORKERS -print-interval $PRINT_INTERVAL
  else
    echo "query file for $queryName not found at /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0.gz"
  fi

  echo "Query $queryName Redis Command Statistics"
  redis-cli info commandstats
done
