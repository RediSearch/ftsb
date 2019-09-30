#!/bin/bash

DATASET="enwiki-latest-abstract1"
#Current revisions only, no talk or user pages; this is probably what you want, and is approximately 14 GB compressed (expands to over 58 GB when decompressed).

MAX_QUERIES=100
WORKERS=8
DEBUG=3

echo ""
echo "---------------------------------------------------------------------------------"
echo "1) $DATASET"
echo "---------------------------------------------------------------------------------"

if [ ! -f /tmp/$DATASET.xml ]; then
  echo "Dataset not found locally. Retrieving from wikimedia."
  curl -O https://dumps.wikimedia.org/enwiki/latest/$DATASET.xml.gz
  gunzip -c $DATASET.xml.gz >/tmp/$DATASET.xml
else
  echo "Dataset found locally at /tmp/$DATASET.xml. No need to retrieve again from from wikimedia."
fi

if [ -f /tmp/$DATASET.xml ]; then
  echo "Issuing ftsb_generate_data."
  ftsb_generate_data -input-file /tmp/$DATASET.xml \
    -format="redisearch" \
    -use-case="enwiki-abstract" |
    gzip >/tmp/ftsb_generate_data-$DATASET-redisearch.gz
else
  echo "ftsb_generate_data file /tmp/ftsb_generate_data-$DATASET-redisearch.gz"
fi

echo ""
echo "---------------------------------------------------------------------------------"
echo "2) $PAGES_DATASET_OUTPUT"
echo "---------------------------------------------------------------------------------"

if [ ! -f /tmp/$PAGES_DATASET_OUTPUT.xml ]; then
  echo "Dataset not found locally. Retrieving from wikimedia."
  curl -O https://dumps.wikimedia.org/enwiki/latest/$PAGES_DATASET
  gunzip -c $PAGES_DATASET >/tmp/$PAGES_DATASET_OUTPUT.xml
else
  echo "Dataset found locally at /tmp/$PAGES_DATASET_OUTPUT.xml. No need to retrieve again from from wikimedia."
fi

if [ -f /tmp/$PAGES_DATASET_OUTPUT.xml ]; then
  echo "Issuing ftsb_generate_data."
  ftsb_generate_data -input-file /tmp/$PAGES_DATASET_OUTPUT.xml \
    -format="redisearch" \
    -use-case="enwiki-pages" |
    gzip >/tmp/ftsb_generate_data-$PAGES_DATASET_OUTPUT-redisearch.gz
else
  echo "ftsb_generate_data file /tmp/ftsb_generate_data-$PAGES_DATASET_OUTPUT-redisearch.gz"
fi
