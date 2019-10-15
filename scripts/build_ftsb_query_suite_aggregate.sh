#!/bin/bash

DATASET="enwiki-latest-pages-articles-multistream"
PRINT_INTERVAL=100000
MAX_QUERIES=100000
WORKERS=8
DEBUG=0

if [ ! -f /tmp/$DATASET.xml ]; then
  echo "Dataset not found locally at /tmp/$DATASET.xml. Aborting."
  exit 1
else
  echo "Dataset found locally at /tmp/$DATASET.xml."
  # "agg-0-*" "agg-1-editor-1year-exact-page-contributions-by-day" "agg-2-*-1month-exact-distinct-editors-by-hour" "agg-3-*-1month-approximate-distinct-editors-by-hour" "agg-4-*-1day-approximate-page-contributions-by-5minutes-by-editor-username" "agg-5-*-1month-approximate-top10-editor-usernames" "agg-6-*-1month-approximate-top10-editor-usernames-by-namespace" "agg-7-*-1month-avg-revision-content-length-by-editor-username" "agg-8-editor-approximate-avg-editor-contributions-by-year"
  for queryName in "agg-0-*"; do
    echo "generating query: $queryName"

    ftsb_generate_queries -query-type=$queryName \
      -queries $MAX_QUERIES -input-file /tmp/$DATASET.xml \
      -debug $DEBUG \
      -seed 12345 \
      -use-case="enwiki-pages" \
      -output-file /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0

    cat /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0 |
      gzip >/tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0.gz

  done
fi
