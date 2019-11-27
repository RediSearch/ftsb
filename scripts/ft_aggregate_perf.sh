#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# DB IP
IP=${IP:-"localhost"}

# DB PORT
PORT=${PORT:-6379}

HOST="$IP:$PORT"

# Index to load the data into
IDX=${IDX:-"agg-test"}


echo ""
echo "---------------------------------------------------------------------------------"
echo "1) Document Ingestion"
echo "---------------------------------------------------------------------------------"

# create the index
redis-cli -h $IP -p $PORT ft.create $IDX SCHEMA \
  TAG_FIELD TAG SORTABLE \
  TEXT_FIELD TEXT SORTABLE

random-string()
{
    cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w ${1:-100} | head -n 1
}

echo ""
echo "---------------------------------------------------------------------------------"
echo "1) Document Ingestion"
echo "---------------------------------------------------------------------------------"

for i in {1..10000} ; do

  # bash generate random 100 character alphanumeric string (upper and lowercase) and
  NEW_UUID_MORE_CHARACTERS=$(openssl rand -hex 1)

  echo ${NEW_UUID_MORE_CHARACTERS}
#  redis-cli -h $IP -p $PORT ft.add $IDX doc${i} 1.0 FIELDS \
#  TAG_FIELD ${NEW_VALUE} \
#  TEXT_FIELD ${NEW_VALUE}
done