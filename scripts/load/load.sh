#!/bin/bash

SUFIX="redisearch-load-${USE_CASE}-w${WORKERS}-maxpipe${PIPELINE}-hashes_${USE_HASHES}-rates-u${UPDATE_RATE}-d${DELETE_RATE}"
echo "Saving results in files starting with: ${SUFIX}"
if [ -f ${DATAFILE_LOADER} ]; then
  cat ${DATAFILE_LOADER} |
    ftsb_load_redisearch \
      -workers=$WORKERS \
      -reporting-period=${REPORTING_PERIOD} \
      -index=$IDX \
      -no-save=${NOSAVE} \
      -host=$HOST \
      -limit=${MAX_INSERTS} \
      -update-rate=${UPDATE_RATE} \
      -replace-partial=${REPLACE_PARTIAL} \
      -replace-condition=${REPLACE_CONDITION} \
      -delete-rate=${DELETE_RATE} \
      -use-case=${USE_CASE} \
      -use-hashes=${USE_HASHES} \
      -debug=${DEBUG} \
      -json-out-file=${SUFIX}.json \
      -batch-size=${BATCH_SIZE} \
      -pipeline=${PIPELINE}
else
  echo "dataset file not found at ${DATAFILE_LOADER}"
fi

redis-cli -h $IP -p $PORT ft.info $IDX >~/${SUFIX}-ft.info.txt
