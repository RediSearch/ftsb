#!/bin/bash

set -e
USE_CASE="synthetic-numeric-int"
DATAFILE_RAW=/tmp/$USE_CASE.xml
DATAFILE_LOADER=/tmp/$USE_CASE.ftsb

# Index to load the databuild into
IDX=${IDX:-"synthetic-numeric-idx1"}
