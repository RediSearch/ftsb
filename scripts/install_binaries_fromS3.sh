#!/bin/bash

# install desired binaries from the public s3 bucket

GOARCH=amd64
GOOS=linux
VERSION=0.2.0
INSTALLPATH="/usr/local/bin"

BUCKET=performance-cto-group-public
BUCKETPATH=benchmarks/redisearch/ftsb/executables
for BINARY in ftsb_generate_data ftsb_generate_queries ftsb_load_redisearch ftsb_run_queries_redisearch; do
  FULLBINARY="${GOOS}_${GOARCH}__v${VERSION}_${BINARY}"
  echo "Getting Binary: $FULLBINARY"
  wget https://$BUCKET.s3.amazonaws.com/$BUCKETPATH/$FULLBINARY
  mv $FULLBINARY $BINARY
  chmod 755 $BINARY
  cp $BINARY "${INSTALLPATH}/${BINARY}"
  echo "Installing Binary: $BINARY in ${INSTALLPATH}/${BINARY}"
done
