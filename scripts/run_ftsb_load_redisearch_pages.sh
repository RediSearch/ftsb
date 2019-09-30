#!/bin/bash

DATASET="enwiki-latest-abstract1"
PAGES_DATASET_OUTPUT="enwiki-latest-pages-articles-multistream"
MAX_QUERIES=100000
PIPELINE=100
DEBUG=0
WORKERS=32
PRINT_INTERVAL=100000
IP="10.3.0.30"
PORT=12000
HOST="$IP:$PORT"
IDX="pages-meta-idx1"

# flush the database
redis-cli flushall

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
redis-cli -h $IP -p $PORT ft.info $IDX 2>&1 | tee ~/redisearch-load-$DATASET-workers-$WORKERS-pipeline-$PIPELINE_ft.info.txt
