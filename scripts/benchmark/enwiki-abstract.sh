#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Load parameters - common
EXE_DIR=${EXE_DIR:-$(dirname $0)}
source ${EXE_DIR}/../common_vars.sh
source ${EXE_DIR}/../usecases/enwiki-abstract.sh

# Run the loader
source ${EXE_DIR}/load.sh
