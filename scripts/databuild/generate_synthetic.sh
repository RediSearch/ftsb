#!/bin/bash

echo "Issuing ftsb_generate_data."
ftsb_generate_data \
  -format=${FORMAT} \
  -use-case=${USE_CASE} \
  -max-documents=${MAX_DOCS} \
  -seed=${SEED} \
  -synthetic-max-dataset-cardinality=${MAX_CARDINALITY} \
  -synthetic-fields=${MAX_FIELDS} \
  -synthetic-field-datasize=${FIELD_SIZE} \
  -debug=${DEBUG} >${DATAFILE_LOADER}

echo "finished generating file ${DATAFILE_LOADER}"
