#!/bin/bash

DATASET="enwiki-latest-abstract1"
REGENERATE_QUERIES="false"
if [[ "${1}" == "true" ]]; then
  REGENERATE_QUERIES="true"
fi

if [ ! -f /tmp/$DATASET.xml ]; then
  echo "Dataset not found locally. Retrieving from wikimedia."
  curl -O https://dumps.wikimedia.org/enwiki/latest/$DATASET.xml.gz
  gunzip -c $DATASET.xml.gz >/tmp/$DATASET.xml
else
  echo "Dataset found locally at /tmp/$DATASET.xml. No need to retrieve again from from wikimedia."
fi

if [ ! -f /tmp/ftsb_generate_data-$DATASET-redisearch.gz ]; then
  echo "ftsb_generate_data file not found. Issuing ftsb_generate_data."
  ftsb_generate_data -input-file /tmp/$DATASET.xml \
    -format="redisearch" |
    gzip >/tmp/ftsb_generate_data-$DATASET-redisearch.gz
else
  echo "ftsb_generate_data file /tmp/ftsb_generate_data-$DATASET-redisearch.gz"
fi

for queryName in "simple-1word-query" "2word-union-query" "2word-intersection-query" "simple-1word-spellcheck"; do
  echo "generating query: $queryName"
  if [ ! -f /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0.gz ] || [[ "$REGENERATE_QUERIES" == "true" ]]; then
    echo "ftsb_generate_queries file for $queryName not found. Issuing ftsb_generate_queries."
    ftsb_generate_queries -query-type=$queryName \
      -queries 100000 -input-file /tmp/$DATASET.xml \
      -seed 12345 \
      -output-file /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0

    cat /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0 |
      gzip >/tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0.gz
  else
    echo "query file for $queryName found at /tmp/redisearch-queries-$DATASET-$queryName-100K-queries-1-0-0.gz."
  fi
done
