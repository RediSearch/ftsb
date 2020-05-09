#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

if [ -f $DATAFILE_RAW ]; then
  echo "Issuing ftsb_generate_data."
  ftsb_generate_data -input-file=${DATAFILE_RAW} \
    -max-documents=${MAX_DOCS} \
    -seed=${SEED} \
    -format=${FORMAT} \
    -use-case=${USE_CASE} -debug=${DEBUG} >${DATAFILE_LOADER}
  echo "finished generating file ${DATAFILE_LOADER}"
else
  echo "Input file not found locally at $DATAFILE_RAW"
fi
