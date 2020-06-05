#!/bin/bash

set -e
USE_CASE="synthetic-text"
DATAFILE_RAW=/tmp/$USE_CASE.xml
DATAFILE_LOADER=/tmp/$USE_CASE.ftsb

# Index to benchmark the databuild into
IDX=${IDX:-"synthetic-text-idx1"}
