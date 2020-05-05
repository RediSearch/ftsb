#!/bin/bash

set -e
#https://dumps.wikimedia.org/enwiki/latest/
#Current revisions only, no talk or user pages;
# this is approximately 14 GB compressed (expands to over 58 GB when decompressed).
DATASETIN="enwiki-latest-pages-articles-multistream12.xml-p3926862p5040436"
REMOTE_URL=https://dumps.wikimedia.org/enwiki/latest/$DATASETIN
USE_CASE="enwiki-pages"
DATAFILE_RAW=/tmp/$USE_CASE.xml
DATAFILE_LOADER=/tmp/$USE_CASE.ftsb

# Index to load the databuild into
IDX=${IDX:-"enwiki-pages-meta-idx1"}

SCHEMA="TITLE TEXT SORTABLE \
  NAMESPACE TAG SORTABLE \
  ID NUMERIC SORTABLE \
  PARENT_REVISION_ID NUMERIC SORTABLE \
  CURRENT_REVISION_TIMESTAMP NUMERIC SORTABLE \
  CURRENT_REVISION_ID NUMERIC SORTABLE \
  CURRENT_REVISION_EDITOR_USERNAME TEXT NOSTEM SORTABLE \
  CURRENT_REVISION_EDITOR_IP TEXT \
  CURRENT_REVISION_EDITOR_USERID NUMERIC SORTABLE \
  CURRENT_REVISION_EDITOR_COMMENT TEXT \
  CURRENT_REVISION_CONTENT_LENGTH NUMERIC SORTABLE"
