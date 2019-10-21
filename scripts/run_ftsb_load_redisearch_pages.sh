#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

DATASET="enwiki-latest-pages"
PAGES_DATASET_OUTPUT="enwiki-latest-pages-articles-multistream"
DEBUG=0
MAX_INSERTS=${MAX_INSERTS:-0}
BATCH_SIZE=${BATCH_SIZE:-1000}
PIPELINE=${PIPELINE:-100}

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

# How many concurrent worker would run queries - match num of cores, or default to 8
WORKERS=${WORKERS:-$(grep -c ^processor /proc/cpuinfo 2>/dev/null || echo 8)}

echo ""
echo "---------------------------------------------------------------------------------"
echo "2) $PAGES_DATASET_OUTPUT"
echo "---------------------------------------------------------------------------------"

# create the index
redis-cli -h $IP -p $PORT ft.create $IDX SCHEMA \
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

#redis-cli -h $IP -p $PORT config resetstat

if [ -f /tmp/ftsb_generate_data-$PAGES_DATASET_OUTPUT-redisearch.gz ]; then
  echo "Using ${WORKERS} WORKERS"
  cat /tmp/ftsb_generate_data-$PAGES_DATASET_OUTPUT-redisearch.gz |
    gunzip |
    ftsb_load_redisearch -workers $WORKERS -reporting-period 1s \
      -index=$IDX \
      -host=$HOST -limit=${MAX_INSERTS} \
      -batch-size ${BATCH_SIZE} -pipeline $PIPELINE -debug=$DEBUG 2>&1 | tee ~/redisearch-load-$DATASET-workers-$WORKERS-pipeline-$PIPELINE.txt
else
  echo "dataset file not found at /tmp/ftsb_generate_data-$PAGES_DATASET_OUTPUT-redisearch.gz"
fi

redis-cli -h $IP -p $PORT ft.info $IDX >~/redisearch-load-$DATASET-workers-$WORKERS-pipeline-$PIPELINE_ft.info.txt
