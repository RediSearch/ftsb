#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

if [ ! -f $DATAFILE_RAW ]; then
  echo "Dataset not found locally at $DATAFILE_RAW. Retrieving it."
  curl -O $REMOTE_URL.gz
  gunzip -c $DATASETIN >$DATAFILE_RAW
#  bzip2 -d $DATASETIN.bz2
#  mv $DATASETIN $DATAFILE_RAW
else
  echo "Dataset found locally at $DATAFILE_RAW. No need to retrieve again."
fi
