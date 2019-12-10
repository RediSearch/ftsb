#!/bin/sh
echo "------------------------------------------------"
echo "Full-Text Search Benchmark (FTSB) - Docker Image"
echo "------------------------------------------------"
echo "Checking if request binary $1 exists"
if [ -f ./$1 ]; then
  ./"$@"
  echo
  echo "...done."
  exit 0
else
  echo "$1 binary does not exist."
  exit 1
fi
