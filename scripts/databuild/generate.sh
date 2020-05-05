#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

if [ ! -f $DATAFILE_RAW ]; then
  echo "Dataset not found locally at $DATAFILE_RAW. Retrieving it."
  curl -O $REMOTE_URL.bz2
  bzip2 -d $DATASETIN.bz2
  mv $DATASETIN $DATAFILE_RAW
else
  echo "Dataset found locally at $DATAFILE_RAW. No need to retrieve again."
fi

if [ -f $DATAFILE_RAW ]; then
  echo "Issuing ftsb_generate_data."
  ftsb_generate_data -input-file=${DATAFILE_RAW} \
    -max-documents=${MAX_DOCS} \
    -seed=${SEED} \
    -format=${FORMAT} \
    -use-case=${USE_CASE} -debug=${DEBUG} >${DATAFILE_LOADER}
  echo "finished generating file ${DATAFILE_LOADER}"
fi
