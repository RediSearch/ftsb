#!/bin/bash
echo "Using ${WORKERS} WORKERS"

for ((WORKERS = $MIN_WORKERS; WORKERS <= $MAX_WORKERS; WORKERS += WORKERS_INCREMENT)); do
  for ((REPETITION = 1; REPETITION <= $REPETITIONS; REPETITION += 1)); do

    # to make the increment correct
    HELPER_MAX_INSERTS=$MAX_INSERTS
    if [[ "${WORKERS}" == "1" ]]; then
      HELPER_MAX_INSERTS=$(($MAX_INSERTS / 10))
    fi

    SUFIX="redisearch-load-${USE_CASE}-cluster{${CLUSTER_MODE}}_w${WORKERS}-maxpipe${PIPELINE}-hashes_${USE_HASHES}-hasprefix_${HAS_PREFIX}_rates-u${UPDATE_RATE}-d${DELETE_RATE}__repetition${REPETITION}"
    echo "Saving results in files starting with: ${SUFIX}"

    echo "Dropping index $IDX if it exists"
    # drop the index if it exists
    redis-cli -h $IP -p $PORT ft.drop $IDX >>/dev/null

    echo "Issuing ft.create $IDX ${EXPRESSION} SCHEMA ${SCHEMA}"

    # create the index
    redis-cli -h $IP -p $PORT ft.create $IDX ${EXPRESSION} SCHEMA ${SCHEMA}

    if [ -f ${DATAFILE_LOADER} ]; then
      cat ${DATAFILE_LOADER} |
        ftsb_load_redisearch \
          -workers=$WORKERS \
          -reporting-period=${REPORTING_PERIOD} \
          -index=$IDX \
          -no-save=${NOSAVE} \
          -host=$HOST \
          -limit=${HELPER_MAX_INSERTS} \
          -update-rate=${UPDATE_RATE} \
          -replace-partial=${REPLACE_PARTIAL} \
          -replace-condition=${REPLACE_CONDITION} \
          -delete-rate=${DELETE_RATE} \
          -use-case=${USE_CASE} \
          -use-hashes=${USE_HASHES} \
          -debug=${DEBUG} \
          -json-out-file=${SUFIX}.json \
          -batch-size=${BATCH_SIZE} \
          -pipeline-max-size=${PIPELINE} \
          -pipeline-window-ms=${PIPELINE_WINDOW_MS} \
                              -cluster-mode=${CLUSTER_MODE}

    else
      echo "dataset file not found at ${DATAFILE_LOADER}"
    fi

    redis-cli -h $IP -p $PORT ft.info $IDX >~/${SUFIX}-ft.info.txt

    echo "Sleeping: $SLEEP_BETWEEN_RUNS"
    sleep ${SLEEP_BETWEEN_RUNS}
  done
  # to make the increment correct
  if [[ "${WORKERS}" == "1" ]]; then
    WORKERS=0
  fi
done
