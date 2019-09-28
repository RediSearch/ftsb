#!/bin/bash

DATASET="enwiki-latest-pages-articles-multistream"
MAX_QUERIES=10000
PIPELINE=1
DEBUG=3
WORKERS=1
PRINT_INTERVAL=10000

MAX_QUERIES=100
WORKERS=8
DEBUG=3

REGENERATE_QUERIES="false"
if [[ "${1}" == "true" ]]; then
  REGENERATE_QUERIES="true"
fi

if [ ! -f /tmp/$DATASET.xml ]; then
  echo "Dataset not found locally. Aborting."
  exit 1
else
  echo "Dataset found locally at /tmp/$DATASET.xml."
  for queryName in "agg-editor-1year-exact-page-contributions-by-day"; do
    echo "generating query: $queryName"
    if [ -f /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0.gz ]; then
      echo "Issuing ftsb_generate_queries."
      ftsb_generate_queries -query-type=$queryName \
        -queries $MAX_QUERIES -input-file /tmp/$DATASET.xml \
        -debug $DEBUG \
        -seed 12345 \
        -use-case="enwiki-pages" \
        -output-file /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0

      cat /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0 |
        gzip >/tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0.gz
    else
      echo "query file for $queryName found at /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0.gz."
    fi
  done
fi
