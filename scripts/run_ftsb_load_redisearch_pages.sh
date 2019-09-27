#!/bin/bash

DATASET="enwiki-latest-abstract1"
PAGES_DATASET_OUTPUT="enwiki-latest-pages-articles-multistream1"
MAX_QUERIES=10000
PIPELINE=1
DEBUG=0
WORKERS=8
PRINT_INTERVAL=10000
IDX="pages-meta-idx1"

# flush the database
redis-cli flushall

echo ""
echo "---------------------------------------------------------------------------------"
echo "2) $PAGES_DATASET_OUTPUT"
echo "---------------------------------------------------------------------------------"

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
      -index=$IDX \
      -batch-size 1000 -pipeline $PIPELINE -debug $DEBUG
else
  echo "dataset file not found at /tmp/ftsb_generate_data-$PAGES_DATASET_OUTPUT-redisearch.gz"
fi

redis-cli info commandstats
redis-cli ft.info $IDX
