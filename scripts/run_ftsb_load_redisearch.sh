#!/bin/bash

DATASET="enwiki-latest-abstract1"
MAX_QUERIES=10000
DEBUG=0
WORKERS=1
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
      -batch-size 1000 -pipeline 100 -debug $DEBUG
else
  echo "dataset file not found at /tmp/ftsb_generate_data-$DATASET-redisearch.gz"
fi

redis-cli info commandstats
redis-cli ft.info idx1
