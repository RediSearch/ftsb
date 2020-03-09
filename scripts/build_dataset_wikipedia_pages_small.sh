#!/bin/bash
#https://dumps.wikimedia.org/enwiki/latest/
#Current revisions only, no talk or user pages;
# this is approximately 14 GB compressed (expands to over 58 GB when decompressed).
DATASET="enwiki-latest-pages-articles-multistream"
DATASETIN="enwiki-latest-pages-articles-multistream12.xml-p3926862p5040436"
SEED=${SEED:-12345}
MAX_DOCS=${MAX_DOCS:-0}
DEBUG=${DEBUG:-3}

echo ""
echo "---------------------------------------------------------------------------------"
echo "2) $PAGES_DATASET_OUTPUT"
echo "---------------------------------------------------------------------------------"

if [ ! -f /tmp/$DATASET.xml ]; then
  echo "Dataset not found locally. Retrieving from wikimedia."
  curl -O https://dumps.wikimedia.org/enwiki/latest/$DATASETIN.bz2
  bzip2 -d $DATASETIN.bz2
  mv $DATASETIN /tmp/$DATASET.xml
else
  echo "Dataset found locally at /tmp/$DATASET.xml. No need to retrieve again from from wikimedia."
fi

if [ -f /tmp/$DATASET.xml ]; then
  echo "Issuing ftsb_generate_data."
  ftsb_generate_data -input-file /tmp/$DATASET.xml \
    -max-documents=${MAX_DOCS} \
    -seed=${SEED} \
    -format="redisearch" \
    -use-case="enwiki-pages" -debug=${DEBUG} |
    gzip >/tmp/ftsb_generate_data-$DATASET-redisearch.gz
  echo "finished generating file /tmp/ftsb_generate_data-$DATASET-redisearch.gz"
else
  echo "ftsb_generate_data file /tmp/ftsb_generate_data-$DATASET-redisearch.gz"
fi
