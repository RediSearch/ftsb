#!/bin/bash

DATASET="enwiki-latest-abstract1"
PAGES_DATASET_OUTPUT="enwiki-latest-pages-articles-multistream1"
MAX_INSERTS=${MAX_INSERTS:-0}

IDX="idx1"
DEBUG=0
PRINT_INTERVAL=10000

# How many concurrent worker would run queries - match num of cores, or default to 8
WORKERS=${WORKERS:-$(grep -c ^processor /proc/cpuinfo 2>/dev/null || echo 8)}

# flush the database
redis-cli flushall

echo ""
echo "---------------------------------------------------------------------------------"
echo "1) $DATASET"
echo "---------------------------------------------------------------------------------"

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
redis-cli ft.info $IDX

echo ""
echo "---------------------------------------------------------------------------------"
echo "2) $PAGES_DATASET_OUTPUT"
echo "---------------------------------------------------------------------------------"

IDX="pages-meta-idx1"

redis-cli ft.drop $IDX SCHEMA

# create the index
redis-cli ft.create $IDX SCHEMA \
  TITLE TEXT SORTABLE \
  NAMESPACE TAG SORTABLE \
  ID NUMERIC SORTABLE \
  PARENT_REVISION_ID NUMERIC SORTABLE \
  CURRENT_REVISION_TIMESTAMP NUMERIC SORTABLE \
  CURRENT_REVISION_ID NUMERIC SORTABLE \
  CURRENT_REVISION_EDITOR_USERNAME TEXT NOSTEM SORTABLE \
  CURRENT_REVISION_EDITOR_IP TEXT \
  CURRENT_REVISION_EDITOR_USERID NUMERIC SORTABLE \
  CURRENT_REVISION_EDITOR_COMMENT TEXT \
  CURRENT_REVISION_CONTENT_LENGTH NUMERIC SORTABLE

redis-cli config resetstat

if [ -f /tmp/ftsb_generate_data-$PAGES_DATASET_OUTPUT-redisearch.gz ]; then
  cat /tmp/ftsb_generate_data-$PAGES_DATASET_OUTPUT-redisearch.gz |
    gunzip |
    ftsb_load_redisearch -workers $WORKERS -reporting-period 1s \
      -batch-size 1000 -pipeline 100 -debug $DEBUG -limit ${MAX_INSERTS}
else
  echo "dataset file not found at /tmp/ftsb_generate_data-$PAGES_DATASET_OUTPUT-redisearch.gz"
fi

redis-cli info commandstats
redis-cli ft.info $IDX
