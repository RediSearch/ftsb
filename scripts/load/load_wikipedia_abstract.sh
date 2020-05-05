#!/bin/bash

DATASET="enwiki-latest-abstract1"

DEBUG=${DEBUG:-0}
MAX_INSERTS=${MAX_INSERTS:-0}
BATCH_SIZE=${BATCH_SIZE:-1000}
PIPELINE=${PIPELINE:-100}
UPDATE_RATE=${UPDATE_RATE:-0.0}
DELETE_RATE=${DELETE_RATE:-0.0}
REPLACE_PARTIAL=${REPLACE_PARTIAL:-false}
REPLACE_CONDITION=${REPLACE_CONDITION:-""}
PRINT_INTERVAL=100000
NOSAVE=${NOSAVE:-"false"}

# DB IP
IP=${IP:-"localhost"}

# DB PORT
PORT=${PORT:-6379}

HOST="$IP:$PORT"

# Index to load the databuild into
IDX=${IDX:-"enwiki-abstract-idx1"}

# How many queries would be run
MAX_QUERIES=${MAX_QUERIES:-100000}

# How many concurrent worker would run queries - match num of cores, or default to 8
WORKERS=${WORKERS:-$(grep -c ^processor /proc/cpuinfo 2>/dev/null || echo 8)}

echo ""
echo "---------------------------------------------------------------------------------"
echo "1) $DATASET"
echo "---------------------------------------------------------------------------------"

redis-cli -h $IP -p $PORT ft.drop $IDX

# create the index
redis-cli -h ${IP} -p ${PORT} ft.create ${IDX} SCHEMA \
  TITLE TEXT WEIGHT 5 SORTABLE \
  URL TEXT WEIGHT 5 SORTABLE \
  ABSTRACT TEXT WEIGHT 1 SORTABLE

if [ -f /tmp/ftsb_generate_data-$DATASET-redisearch.gz ]; then
  SUFIX="redisearch-load-${DATASET}-w${WORKERS}-pipe${PIPELINE}-RATES-u${UPDATE_RATE}-d${DELETE_RATE}"
  cat /tmp/ftsb_generate_data-$DATASET-redisearch.gz |
    gunzip |
    ftsb_load_redisearch -workers $WORKERS -reporting-period 1s \
      -index=$IDX \
      -no-save=${NOSAVE} \
      -host=$HOST -limit=${MAX_INSERTS} \
      -update-rate=${UPDATE_RATE} \
      -replace-partial=${REPLACE_PARTIAL} \
      -replace-condition=${REPLACE_CONDITION} \
      -delete-rate=${DELETE_RATE} \
      -use-case="enwiki-abstract" \
      -use-hashes=${USE_HASHES} \
      -debug=${DEBUG} \
      -json-out-file=${SUFIX}.json \
      -batch-size=${BATCH_SIZE} -pipeline=$PIPELINE
else
  echo "dataset file not found at /tmp/ftsb_generate_data-$DATASET-redisearch.gz"
fi

redis-cli -h $IP -p $PORT ft.info $IDX >~/${SUFIX}-ft.info.txt
