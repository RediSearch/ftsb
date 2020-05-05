#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Load parameters - common
EXE_DIR=${EXE_DIR:-$(dirname $0)}
source ${EXE_DIR}/../common_vars.sh
source ${EXE_DIR}/../usecases/enwiki-pages.sh

# drop the index if it exists
redis-cli -h $IP -p $PORT ft.drop $IDX >>/dev/null

# create the index
redis-cli -h $IP -p $PORT ft.create $IDX SCHEMA ${SCHEMA}

# Run the loader
source ${EXE_DIR}/load.sh
