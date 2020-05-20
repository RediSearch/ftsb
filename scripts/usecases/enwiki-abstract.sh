#!/bin/bash

set -e
#https://dumps.wikimedia.org/enwiki/latest/
DATASETIN="enwiki-latest-abstract1.xml"
REMOTE_URL=https://dumps.wikimedia.org/enwiki/latest/$DATASETIN
USE_CASE="enwiki-abstract"
DATAFILE_RAW=/tmp/$USE_CASE.xml
DATAFILE_LOADER=/tmp/$USE_CASE.ftsb

# Index to load the databuild into
IDX=${IDX:-"enwiki-abstract-idx1"}

SCHEMA="TITLE TEXT WEIGHT 5 SORTABLE \
  URL TEXT WEIGHT 5 SORTABLE \
  ABSTRACT TEXT WEIGHT 1 SORTABLE"

EXPRESSION=""
if [[ "${HAS_PREFIX}" == "true" && "${USE_HASHES}" == "true" ]]; then
  EXPRESSION=" EXPRESSION hasprefix(\"enwiki-abstract\")"
fi
