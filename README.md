# Full-Text Search Benchmark (FTSB)
This repo contains code for benchmarking full text search databases,
including RediSearch.
This code is based on a fork of work initially made public by TSBS
at https://github.com/timescale/tsbs.

Current databases supported:

+ RediSearch [(supplemental docs)](docs/redisearch.md)

## Overview

The **Full-Text Search Benchmark (FTSB)** is a collection of Go
programs that are used to generate datasets and then benchmark read
and write performance of various databases. The intent is to make the
FTSB extensible so that a variety of use cases (e.g., wikipedia, jsondata,
etc.), query types, and databases can be included and benchmarked.
To this end we hope to help prospective database administrators find the
best database for their needs and their workloads. Further, if you
are the developer of a Full-Text Search database and want to include your
database in the FTSB, feel free to open a pull request to add it!

## Current use cases

Currently, FTSB supports one use case -- enwiki-abstract, From English-language Wikipedia:Database page abstracts. This use case generates
3 Text fields per document.


## What the FTSB tests

FTSB is used to benchmark bulk load performance and
query execution performance. 
To accomplish this in a fair way, the data to be inserted and the
queries to run are pre-generated and native Go clients are used
wherever possible to connect to each database.


## Installation

FTSB is a collection of Go programs (with some auxiliary bash and Python
scripts). The easiest way to get and install the Go programs is to use
`go get` and then `go install`:
```bash
# Fetch FTSB and its dependencies
$ go get github.com/filipecosta90/ftsb
$ cd $GOPATH/src/github.com/filipecosta90/ftsb/cmd
$ go get ./...

# Install desired binaries. At a minimum this includes ftsb_generate_data,
# ftsb_generate_queries, one ftsb_load_* binary, and one ftsb_run_queries_*
# binary:
$ cd $GOPATH/src/github.com/filipecosta90/ftsb/cmd
$ cd ftsb_generate_data && go install
$ cd ../ftsb_generate_queries && go install
$ cd ../ftsb_load_redisearch && go install
$ cd ../ftsb_run_queries_redisearch && go install

```

## How to use FTSB

Using FTSB for benchmarking involves 3 phases: data and query
generation, data loading/insertion, and query execution.

### Data and query generation

So that benchmarking results are not affected by generating data or
queries on-the-fly, with FTSB you generate the data and queries you want
to benchmark first, and then you can (re-)use it as input to the
benchmarking phases.

#### Data generation

Variables needed:
1. a use case. E.g., `enwiki-abstract` (currently only `enwiki-abstract`)
1. the file from which to read the data from, compliant with the use case. E.g. `enwiki-latest-abstract1.xml.gz`
1. and which database(s) you want to generate for. E.g., `redisearch` (currently only `redisearch`)

Given the above steps you can now generate a dataset (or multiple
datasets, if you chose to generate for multiple databases) that can
be used to benchmark data loading of the database(s) chosen using
the `tsbs_generate_data` tool:
```bash
$ curl -O https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-abstract1.xml.gz
$ gunzip enwiki-latest-abstract1.xml.gz
$ ./ftsb_generate_data -input-file enwiki-latest-abstract1.xml \
     -format="redisearch" \
    | gzip > enwiki-latest-abstract1.gz 

# Each additional database would be a separate call.
```
_Note: We pipe the output to gzip to reduce on-disk space._


#### Query generation

TBD

### Benchmarking insert/write performance

TBD

### Benchmarking query execution performance

TBD