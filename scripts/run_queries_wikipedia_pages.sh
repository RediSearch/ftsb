#!/bin/bash

DATASET="enwiki-latest-pages-articles-multistream"

PIPELINE=${PIPELINE:-1}
DEBUG=${DEBUG:-0}
PRINT_INTERVAL=${PRINT_INTERVAL:-100000}

# DB IP
IP=${IP:-"localhost"}

# DB PORT
PORT=${PORT:-6379}

HOST="$IP:$PORT"

# Index to load the data into
IDX=${IDX:-"pages-meta-idx1"}

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
echo "Using ${WORKERS} WORKERS"
#for queryName in  "agg-0-*" "agg-1-editor-1year-exact-page-contributions-by-day" "agg-2-*-1month-exact-distinct-editors-by-hour" "agg-3-*-1month-approximate-distinct-editors-by-hour" "agg-4-*-1day-approximate-page-contributions-by-5minutes-by-editor-username" "agg-5-*-1month-approximate-top10-editor-usernames" "agg-6-*-1month-approximate-top10-editor-usernames-by-namespace" "agg-7-*-1month-avg-revision-content-length-by-editor-username" "agg-8-editor-approximate-avg-editor-contributions-by-year"; do
for queryName in "agg-1-editor-1year-exact-page-contributions-by-day" "agg-2-*-1month-exact-distinct-editors-by-hour" "agg-3-*-1month-approximate-distinct-editors-by-hour" "agg-4-*-1day-approximate-page-contributions-by-5minutes-by-editor-username" "agg-5-*-1month-approximate-top10-editor-usernames" "agg-6-*-1month-approximate-top10-editor-usernames-by-namespace" "agg-7-*-1month-avg-revision-content-length-by-editor-username" "agg-8-editor-approximate-avg-editor-contributions-by-year"; do
  echo "Benchmarking query: $queryName"

  if [ -f /tmp/redisearch-queries-$DATASET-$queryName-${MAX_QUERIES}-queries-1-0-0.gz ]; then
    cat /tmp/redisearch-queries-$DATASET-$queryName-${MAX_QUERIES}-queries-1-0-0.gz |
      gunzip >/tmp/redisearch-queries-$DATASET-$queryName-${MAX_QUERIES}-queries-1-0-0

    ftsb_run_queries_redisearch \
      -file /tmp/redisearch-queries-$DATASET-$queryName-${MAX_QUERIES}-queries-1-0-0 \
      -index=${IDX} \
      -host=${HOST} \
      -limit-rps=${RATE_LIMIT} \
      -debug=${DEBUG} \
      -max-queries=${MAX_QUERIES} -with-cursor=${WITH_CURSOR} \
      -output-file-stats-hdr-response-latency-hist=~/HDR-redisearch-queries-$DATASET-$queryName-${MAX_QUERIES}-queries-1-0-0.txt \
      -workers=${WORKERS} -print-interval=${PRINT_INTERVAL} 2>&1 | tee ~/redisearch-queries-$DATASET-$queryName-${MAX_QUERIES}-queries-1-0-0-RATE_LIMIT-${RATE_LIMIT}.txt

    redis-cli -h $IP -p $PORT ft.info $IDX >~/redisearch-queries-$DATASET-$queryName-${MAX_QUERIES}-queries-1-0-0_ft.info.txt
    echo "HDR Latency Histogram for Query $queryName saved at ~/HDR-redisearch-queries-$DATASET-$queryName-${MAX_QUERIES}-queries-1-0-0-RATE_LIMIT-${RATE_LIMIT}.txt"

    sleep ${SLEEP_BETWEEN_RUNS}

  else
    echo "query file for $queryName not found at /tmp/redisearch-queries-$DATASET-$queryName-${MAX_QUERIES}-queries-1-0-0-RATE_LIMIT-${RATE_LIMIT}.gz"
  fi

done
echo "Benchmarking WITH_CURSOR query execution performance"

MAX_QUERIES=1000
WITH_CURSOR=true

for queryName in "agg-0-*"; do
  echo "Benchmarking query: $queryName"

  if [ -f /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0.gz ]; then
    cat /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0.gz |
      gunzip >/tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0

    ftsb_run_queries_redisearch \
      -file /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0 \
      -index=${IDX} \
      -host=${HOST} \
      -limit-rps=${RATE_LIMIT} \
      -max-queries ${MAX_QUERIES} -with-cursor=${WITH_CURSOR} \
      -output-file-stats-hdr-response-latency-hist ~/HDR-WITH_CURSOR-redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0-RATE_LIMIT-${RATE_LIMIT}.txt \
      -workers ${WORKERS} -print-interval ${PRINT_INTERVAL} 2>&1 | tee ~/WITH_CURSOR-redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0-RATE_LIMIT-${RATE_LIMIT}.txt
  else
    echo "query file for $queryName not found at /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0-RATE_LIMIT-${RATE_LIMIT}.gz"
  fi
  sleep ${SLEEP_BETWEEN_RUNS}
done

echo "Benchmarking WITH_CURSOR false query execution performance"

MAX_QUERIES=1000
WITH_CURSOR=false

for queryName in "agg-0-*"; do
  echo "Benchmarking query: $queryName"

  if [ -f /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0.gz ]; then
    cat /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0.gz |
      gunzip >/tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0

    ftsb_run_queries_redisearch \
      -file /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0 \
      -index=${IDX} \
      -host=${HOST} \
      -limit-rps=${RATE_LIMIT} \
      -max-queries ${MAX_QUERIES} -with-cursor=${WITH_CURSOR} \
      -output-file-stats-hdr-response-latency-hist ~/HDR-WITH_CURSOR-false-redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0-RATE_LIMIT-${RATE_LIMIT}.txt \
      -workers ${WORKERS} -print-interval ${PRINT_INTERVAL} 2>&1 | tee ~/WITH_CURSOR-false-redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0-RATE_LIMIT-${RATE_LIMIT}.txt
  else
    echo "query file for $queryName not found at /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0-RATE_LIMIT-${RATE_LIMIT}.gz"
  fi
done
