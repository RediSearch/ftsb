#!/bin/bash

DATASET="enwiki-latest-pages-articles-multistream"
PIPELINE=1
DEBUG=0
PRINT_INTERVAL=100000

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
SLEEP_BETWEEN_RUNS=${SLEEP_BETWEEN_RUNS:-60}

# How many concurrent worker would run queries - match num of cores, or default to 8
WORKERS=${WORKERS:-$(grep -c ^processor /proc/cpuinfo 2>/dev/null || echo 8)}

IDX="pages-meta-idx1"
#QUERIES="agg-1-editor-1year-exact-page-contributions-by-day" # "agg-2-*-1month-exact-distinct-editors-by-hour" "agg-3-*-1month-approximate-distinct-editors-by-hour" "agg-4-*-1day-approximate-page-contributions-by-5minutes-by-editor-username" "agg-5-*-1month-approximate-top10-editor-usernames" "agg-6-*-1month-approximate-top10-editor-usernames-by-namespace" "agg-7-*-1month-avg-revision-content-length-by-editor-username" "agg-8-editor-approximate-avg-editor-contributions-by-year"
#QUERIES="agg-7-*-1month-avg-revision-content-length-by-editor-username" "agg-8-editor-approximate-avg-editor-contributions-by-year"

echo "Benchmarking query execution performance"
for queryName in "agg-8-editor-approximate-avg-editor-contributions-by-year"; do
  echo "Benchmarking query: $queryName"
  redis-cli -h $IP -p $PORT config resetstat

  if [ -f /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0.gz ]; then
    cat /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0.gz |
      gunzip >/tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0

    ftsb_run_queries_redisearch \
      -file /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0 \
      -index=$IDX \
      -host=$HOST \
      -max-queries $MAX_QUERIES -workers $WORKERS -print-interval $PRINT_INTERVAL 2>&1 | tee ~/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0.txt
  else
    echo "query file for $queryName not found at /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0.gz"
  fi

  echo "Query $queryName Redis Command Statistics"
  redis-cli -h $IP -p $PORT info commandstats 2>&1 | tee ~/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0_commandstats.txt

  redis-cli -h $IP -p $PORT ft.info $IDX >~/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0_ft.info.txt

  sleep $SLEEP_BETWEEN_RUNS

done
