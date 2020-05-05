#!/bin/bash

DATASET="synthetic-tag"
#Current revisions only, no talk or user pages; this is probably what you want, and is approximately 14 GB compressed (expands to over 58 GB when decompressed).

DEBUG=${DEBUG:-3}
MAX_DOCS=${MAX_DOCS:-1000000}
MAX_CARDINALITY=${MAX_CARDINALITY:-65536}
FIELD_SIZE=${FIELD_SIZE:-64}
SEED=${SEED:-12345}

# RediSearch supports up to 1024 fields per schema, out of which at most 128 can be TEXT fields.
# On 32 bit builds, at most 64 fields can be TEXT fields.
MAX_FIELDS=${MAX_FIELDS:-10}

# How many concurrent worker would run queries - match num of cores, or default to 8
WORKERS=${WORKERS:-$(grep -c ^processor /proc/cpuinfo 2>/dev/null || echo 8)}

echo ""
echo "---------------------------------------------------------------------------------"
echo "1) $DATASET"
echo "---------------------------------------------------------------------------------"

echo "Issuing ftsb_generate_data."
ftsb_generate_data \
  -format="redisearch" \
  -max-documents=${MAX_DOCS} \
  -seed=${SEED} \
  -synthetic-max-dataset-cardinality=${MAX_CARDINALITY} \
  -synthetic-fields=${MAX_FIELDS} \
  -synthetic-field-datasize=${FIELD_SIZE} \
  -use-case="synthetic-tag" -debug=${DEBUG} |
  gzip >/tmp/ftsb_generate_data-$DATASET-redisearch.gz

echo "Data saved to /tmp/ftsb_generate_data-$DATASET-redisearch.gz."
