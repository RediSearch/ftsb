#!/bin/bash

DATASET="enwiki-latest-abstract1"
#Current revisions only, no talk or user pages; this is probably what you want, and is approximately 14 GB compressed (expands to over 58 GB when decompressed).
PAGES_DATASET="enwiki-latest-pages-articles-multistream1.xml-p10p30302.bz2"

MAX_QUERIES=100
WORKERS=8
DEBUG=3


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
    -format="redisearch" |
    gzip >/tmp/ftsb_generate_data-$DATASET-redisearch.gz
else
  echo "ftsb_generate_data file /tmp/ftsb_generate_data-$DATASET-redisearch.gz"
fi
