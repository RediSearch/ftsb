#!/bin/bash

DATASET="enwiki-latest-pages-articles-multistream"
PIPELINE=1
DEBUG=0
PRINT_INTERVAL=1000
MAX_QUERIES=1000
WORKERS=8
IP="10.3.0.30"
PORT=12000
HOST="$IP:$PORT"

IDX="pages-meta-idx1"

echo "Benchmarking query execution performance"
for queryName in "agg-1-editor-1year-exact-page-contributions-by-day" "agg-2-*-1month-exact-distinct-editors-by-hour" "agg-3-*-1month-approximate-distinct-editors-by-hour" "agg-4-*-1day-approximate-page-contributions-by-5minutes-by-editor-username" "agg-5-*-1month-approximate-top10-editor-usernames" "agg-6-*-1month-approximate-top10-editor-usernames-by-namespace" "agg-7-*-1month-avg-revision-content-length-by-editor-username" "agg-8-editor-approximate-avg-editor-contributions-by-year"; do
  echo "Benchmarking query: $queryName"
  redis-cli -h $IP -p $PORT config resetstat

  if [ -f /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0.gz ]; then
    cat /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0.gz |
      gunzip >/tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0

    ftsb_run_queries_redisearch \
      -file /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0 \
      -index=$IDX \
      -host=$HOST \
      -max-queries $MAX_QUERIES -workers $WORKERS -print-interval $PRINT_INTERVAL
  else
    echo "query file for $queryName not found at /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0.gz"
  fi

  echo "Query $queryName Redis Command Statistics"
  redis-cli -h $IP -p $PORT info commandstats
done
