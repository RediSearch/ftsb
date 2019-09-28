#!/bin/bash

DATASET="enwiki-latest-pages-articles-multistream"
PIPELINE=1
DEBUG=3
PRINT_INTERVAL=10000
MAX_QUERIES=100
WORKERS=8
IDX="pages-meta-idx1"

echo "Benchmarking query execution performance"
for queryName in "agg-*-aproximate-top10-editor-usernames-by-namespace" "agg-*-avg-revision-content-length-by-editor-username" "agg-editor-1year-exact-page-contributions-by-day"; do
  echo "Benchmarking query: $queryName"
  redis-cli config resetstat

  if [ -f /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0.gz ]; then
    cat /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0.gz |
      gunzip >/tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0

    ftsb_run_queries_redisearch \
      -file /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0 \
      -index=$IDX \
      -max-queries $MAX_QUERIES -workers $WORKERS -print-interval $PRINT_INTERVAL
  else
    echo "query file for $queryName not found at /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0.gz"
  fi

  echo "Query $queryName Redis Command Statistics"
  redis-cli info commandstats
done
