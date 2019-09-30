#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

DATASET="enwiki-latest-abstract1"
PAGES_DATASET_OUTPUT="enwiki-latest-pages-articles-multistream"
PIPELINE=100
DEBUG=0

PRINT_INTERVAL=100000

# DB IP
IP=${IP:-"10.3.0.30"}

# DB PORT
PORT=${PORT:-12000}

HOST="$IP:$PORT"

# Index to load the data into
IDX=${IDX:-"pages-meta-idx1"}

# How many queries would be run
MAX_QUERIES=${MAX_QUERIES:-100000}

# How many concurrent worker would run queries - match num of cores, or default to 8
WORKERS=${WORKERS:-$(grep -c ^processor /proc/cpuinfo 2> /dev/null || echo 8)}


# flush the database
redis-cli -h $IP -p $PORT flushall

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

redis-cli -h $IP -p $PORT config resetstat

if [ -f /tmp/ftsb_generate_data-$PAGES_DATASET_OUTPUT-redisearch.gz ]; then
  cat /tmp/ftsb_generate_data-$PAGES_DATASET_OUTPUT-redisearch.gz |
    gunzip |
    ftsb_load_redisearch -workers $WORKERS -reporting-period 1s \
      -index=$IDX \
      -host=$HOST \
      -batch-size 1000 -pipeline $PIPELINE -debug=$DEBUG 2>&1 | tee ~/redisearch-load-$DATASET-workers-$WORKERS-pipeline-$PIPELINE.txt
else
  echo "dataset file not found at /tmp/ftsb_generate_data-$PAGES_DATASET_OUTPUT-redisearch.gz"
fi

redis-cli -h $IP -p $PORT info commandstats 2>&1 | tee ~/redisearch-load-$DATASET-workers-$WORKERS-pipeline-$PIPELINE_commandstats.txt
redis-cli -h $IP -p $PORT ft.info $IDX > ~/redisearch-load-$DATASET-workers-$WORKERS-pipeline-$PIPELINE_ft.info.txt
