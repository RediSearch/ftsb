#!/bin/bash
# Load parameters - common
EXE_DIR=${EXE_DIR:-$(dirname $0)}
source ${EXE_DIR}/../common_vars.sh
source ${EXE_DIR}/../usecases/synthetic_tag.sh

# Run the loader
source ${EXE_DIR}/load.sh
