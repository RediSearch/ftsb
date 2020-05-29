#!/bin/bash

set -e
#https://dumps.wikimedia.org/enwiki/latest/
DATASETIN="enwiki-latest-abstract1.xml"
REMOTE_URL=https://dumps.wikimedia.org/enwiki/latest/$DATASETIN
USE_CASE="enwiki-abstract"
DATAFILE_RAW=/tmp/$USE_CASE.xml
DATAFILE_LOADER=/tmp/$USE_CASE.txt
DATAFILE_JSON_CONFIG=/tmp/$USE_CASE.config.json



