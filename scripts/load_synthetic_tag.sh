#!/bin/bash

# RediSearch supports up to 1024 fields per schema, out of which at most 128 can be TEXT fields.
# On 32 bit builds, at most 64 fields can be TEXT fields.

# Exit immediately if a command exits with a non-zero status.
set -e

PRINT_INTERVAL=100000
DEBUG=${DEBUG:-0}
MAX_INSERTS=${MAX_INSERTS:-0}
BATCH_SIZE=${BATCH_SIZE:-1000}
PIPELINE=${PIPELINE:-10}
UPDATE_RATE=${UPDATE_RATE:-0.0}
DELETE_RATE=${DELETE_RATE:-0.0}
DATASET="synthetic-tag"
MAX_CARDINALITY=${MAX_CARDINALITY:-65536}
MAX_FIELDS=${MAX_FIELDS:-10}
# Index to load the data into
IDX=${IDX:-"synthetic-tag-idx1"}

# DB IP
IP=${IP:-"localhost"}

# DB PORT
PORT=${PORT:-6379}

HOST="$IP:$PORT"

# How many queries would be run
REPORTING_PERIOD=${REPORTING_PERIOD:-"1s"}

# How many concurrent worker would run queries - match num of cores, or default to 8
WORKERS=${WORKERS:-$(grep -c ^processor /proc/cpuinfo 2>/dev/null || echo 8)}

echo ""
echo "---------------------------------------------------------------------------------"
echo "2) $DATASET"
echo "---------------------------------------------------------------------------------"

if [ -f /tmp/ftsb_generate_data-$DATASET-redisearch.gz ]; then
  echo "Using ${WORKERS} WORKERS"
  cat /tmp/ftsb_generate_data-$DATASET-redisearch.gz |
    gunzip |
    ftsb_load_redisearch -workers=$WORKERS \
      -reporting-period=${REPORTING_PERIOD} \
      -index=$IDX \
      -host=$HOST -limit=${MAX_INSERTS} \
      -update-rate=${UPDATE_RATE} \
      -delete-rate=${DELETE_RATE} \
      -synthetic-max-dataset-cardinality=${MAX_CARDINALITY} \
      -synthetic-fields=${MAX_FIELDS} \
      -use-case="synthetic-tag" \
      -debug=${DEBUG} \
      -batch-size=${BATCH_SIZE} -pipeline=$PIPELINE -debug=$DEBUG 2>&1 | tee ~/redisearch-load-RATES-UPD-${UPDATE_RATE}-DEL-${DELETE_RATE}-$DATASET-workers-$WORKERS-pipeline-$PIPELINE.txt
else
  echo "dataset file not found at /tmp/ftsb_generate_data-$DATASET-redisearch.gz"
fi

redis-cli -h $IP -p $PORT ft.info $IDX >~/redisearch-load-$DATASET-workers-$WORKERS-pipeline-$PIPELINE_ft.info.txt
