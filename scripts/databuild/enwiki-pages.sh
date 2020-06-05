#!/bin/bash

# Load parameters - common
EXE_DIR=${EXE_DIR:-$(dirname $0)}
source ${EXE_DIR}/../common_vars.sh
source ${EXE_DIR}/../usecases/enwiki-pages.sh
source ${EXE_DIR}/get_bz2.sh
source ${EXE_DIR}/generate.sh
