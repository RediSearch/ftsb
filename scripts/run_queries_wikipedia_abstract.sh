#!/bin/bash

DATASET="enwiki-latest-abstract1"

PIPELINE=${PIPELINE:-1}
DEBUG=${DEBUG:-0}
PRINT_INTERVAL=${PRINT_INTERVAL:-100000}

# DB IP
IP=${IP:-"localhost"}

# DB PORT
PORT=${PORT:-6379}

HOST="$IP:$PORT"

# Index to load/query data to/from
IDX=${IDX:-"idx1"}

# How many queries would be run
MAX_QUERIES=${MAX_QUERIES:-100000}

# How many queries would be run
WITH_CURSOR=${WITH_CURSOR:-false}

# Rate limit? if greater than 0 rate is limited.
RATE_LIMIT=${RATE_LIMIT:-0}

# How many queries would be run
SLEEP_BETWEEN_RUNS=${SLEEP_BETWEEN_RUNS:-60}

# How many concurrent worker would run queries - match num of cores, or default to 8
WORKERS=${WORKERS:-$(grep -c ^processor /proc/cpuinfo 2>/dev/null || echo 8)}

echo "Benchmarking query execution performance"
# "simple-1word-spellcheck"
for queryName in "simple-1word-query" "2word-union-query" "2word-intersection-query"; do
  echo "Benchmarking query: $queryName"

  if [ -f /tmp/redisearch-queries-$DATASET-$queryName-${MAX_QUERIES}-queries-1-0-0.gz ]; then
    cat /tmp/redisearch-queries-$DATASET-$queryName-${MAX_QUERIES}-queries-1-0-0.gz |
      gunzip >/tmp/redisearch-queries-$DATASET-$queryName-${MAX_QUERIES}-queries-1-0-0

    ftsb_run_queries_redisearch \
      -file=/tmp/redisearch-queries-$DATASET-$queryName-${MAX_QUERIES}-queries-1-0-0 \
      -max-queries=${MAX_QUERIES} \
      -index=${IDX} \
      -host=${HOST} \
      -limit-rps=${RATE_LIMIT} \
      -output-file-stats-hdr-response-latency-hist=~/HDR-redisearch-queries-$DATASET-$queryName-${MAX_QUERIES}-queries-1-0-0-RATE_LIMIT-${RATE_LIMIT}.txt \
      -workers=${WORKERS} -print-interval=${PRINT_INTERVAL} 2>&1 | tee ~/redisearch-queries-$DATASET-$queryName-${MAX_QUERIES}-queries-1-0-0-RATE_LIMIT-${RATE_LIMIT}.txt

    echo "HDR Latency Histogram for Query $queryName saved at ~/HDR-redisearch-queries-$DATASET-$queryName-${MAX_QUERIES}-queries-1-0-0-RATE_LIMIT-${RATE_LIMIT}.txt"
    sleep ${SLEEP_BETWEEN_RUNS}

  else
    echo "query file for $queryName not found at /tmp/redisearch-queries-$DATASET-$queryName-${MAX_QUERIES}-queries-1-0-0-RATE_LIMIT-${RATE_LIMIT}.gz"
  fi

done
