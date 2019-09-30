#!/bin/bash
#https://dumps.wikimedia.org/enwiki/latest/
#Current revisions only, no talk or user pages;
# this is approximately 14 GB compressed (expands to over 58 GB when decompressed).
DATASET="enwiki-latest-pages-articles-multistream"
DATASETIN="enwiki-latest-pages-articles-multistream12.xml-p3926864p5040435"
MAX_QUERIES=0
WORKERS=8
DEBUG=3

echo ""
echo "---------------------------------------------------------------------------------"
echo "2) $PAGES_DATASET_OUTPUT"
echo "---------------------------------------------------------------------------------"

if [ ! -f /tmp/$DATASET.xml ]; then
  echo "Dataset not found locally. Retrieving from wikimedia."
  curl -O https://dumps.wikimedia.org/enwiki/latest/$DATASETIN.bz2
  gunzip -c $DATASETIN.bz2 >/tmp/$DATASET.xml
else
  echo "Dataset found locally at /tmp/$DATASET.xml. No need to retrieve again from from wikimedia."
fi

if [ -f /tmp/$DATASET.xml ]; then
  echo "Issuing ftsb_generate_data."
  ftsb_generate_data -input-file /tmp/$DATASET.xml \
    -format="redisearch" \
    -use-case="enwiki-pages" -debug=$DEBUG |
    gzip >/tmp/ftsb_generate_data-$DATASET-redisearch.gz
  echo "finished generating file /tmp/ftsb_generate_data-$DATASET-redisearch.gz"
else
  echo "ftsb_generate_data file /tmp/ftsb_generate_data-$DATASET-redisearch.gz"
fi
