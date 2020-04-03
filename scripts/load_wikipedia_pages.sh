#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

DATASET="enwiki-latest-pages"
PAGES_DATASET_OUTPUT="enwiki-latest-pages-articles-multistream"
DEBUG=${DEBUG:-0}
MAX_INSERTS=${MAX_INSERTS:-0}
BATCH_SIZE=${BATCH_SIZE:-1000}
PIPELINE=${PIPELINE:-100}
UPDATE_RATE=${UPDATE_RATE:-0.0}
REPLACE_PARTIAL=${REPLACE_PARTIAL:-false}
REPLACE_CONDITION=${REPLACE_CONDITION:-""}
DELETE_RATE=${DELETE_RATE:-0.0}
NOSAVE=${NOSAVE:-"false"}

PRINT_INTERVAL=100000

# DB IP
IP=${IP:-"localhost"}

# DB PORT
PORT=${PORT:-6379}

HOST="$IP:$PORT"

# Index to load the data into
IDX=${IDX:-"enwiki-pages-meta-idx1"}

# How many queries would be run
MAX_QUERIES=${MAX_QUERIES:-100000}

# How many queries would be run
REPORTING_PERIOD=${REPORTING_PERIOD:-"1s"}

# How many concurrent worker would run queries - match num of cores, or default to 8
WORKERS=${WORKERS:-$(grep -c ^processor /proc/cpuinfo 2>/dev/null || echo 8)}

echo ""
echo "---------------------------------------------------------------------------------"
echo "2) $PAGES_DATASET_OUTPUT"
echo "---------------------------------------------------------------------------------"

redis-cli -h $IP -p $PORT ft.drop $IDX

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

if [ -f /tmp/ftsb_generate_data-$PAGES_DATASET_OUTPUT-redisearch.gz ]; then
  echo "Using ${WORKERS} WORKERS"
  SUFIX="redisearch-load-${DATASET}-w${WORKERS}-pipe${PIPELINE}-RATES-u${UPDATE_RATE}-d${DELETE_RATE}"
  cat /tmp/ftsb_generate_data-$PAGES_DATASET_OUTPUT-redisearch.gz |
    gunzip |
    ftsb_load_redisearch -workers=$WORKERS \
      -reporting-period=${REPORTING_PERIOD} \
      -index=$IDX \
      -no-save=${NOSAVE} \
      -host=$HOST -limit=${MAX_INSERTS} \
      -update-rate=${UPDATE_RATE} \
      -replace-partial=${REPLACE_PARTIAL} \
      -replace-condition=${REPLACE_CONDITION} \
      -delete-rate=${DELETE_RATE} \
      -use-case="enwiki-pages" \
      -debug=${DEBUG} \
      -json-out-file=${SUFIX}.json \
      -batch-size=${BATCH_SIZE} -pipeline=$PIPELINE
else
  echo "dataset file not found at /tmp/ftsb_generate_data-$DATASET-redisearch.gz"
fi

redis-cli -h $IP -p $PORT ft.info $IDX >~/${SUFIX}-ft.info.txt
