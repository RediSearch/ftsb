#!/bin/bash

# Install desired binaries. At a minimum this includes ftsb_generate_data,
# ftsb_generate_queries, one ftsb_load_* binary, and one ftsb_run_queries_*
# binary:
cd $GOPATH/src/github.com/RediSearch/ftsb/cmd
cd ftsb_generate_data && go install
cd ../ftsb_generate_queries && go install
cd ../ftsb_load_redisearch && go install
cd ../ftsb_run_queries_redisearch && go install
cd $GOPATH/src/github.com/RediSearch/ftsb