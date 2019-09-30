#!/bin/bash

# build and upload to public s3 bucket the desired binaries

GOARCH=amd64
VERSION=0.2.0
BUCKETPATH=performance-cto-group-public/benchmarks/redisearch/ftsb/executables
for GOOS in linux darwin; do
  echo "Building for OS: $GOOS"

  for BINARY in ftsb_generate_data ftsb_generate_queries ftsb_load_redisearch ftsb_run_queries_redisearch; do

    cd $GOPATH/src/github.com/RediSearch/ftsb/cmd/$BINARY
    FULLBINARY="${GOOS}_${GOARCH}__v${VERSION}_${BINARY}"
    echo "Building Binary: $FULLBINARY"
    env GOOS=$GOOS GOARCH=$GOARCH go build
    cp $BINARY $FULLBINARY
    echo "Uploading Binary: $FULLBINARY to s3://$BUCKETPATH/$FULLBINARY"

    aws s3 cp $GOPATH/src/github.com/RediSearch/ftsb/cmd/$BINARY/$FULLBINARY s3://$BUCKETPATH/$FULLBINARY --acl public-read
    echo "Removing local Binary: $FULLBINARY"
    rm $FULLBINARY

  done
done

cd $GOPATH/src/github.com/RediSearch/ftsb
