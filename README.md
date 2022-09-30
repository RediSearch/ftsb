[![license](https://img.shields.io/github/license/RediSearch/ftsb.svg)](https://github.com/RediSearch/ftsb)
[![CircleCI](https://circleci.com/gh/RediSearch/ftsb/tree/master.svg?style=svg)](https://circleci.com/gh/RediSearch/ftsb/tree/master)
[![GitHub issues](https://img.shields.io/github/release/RediSearch/ftsb.svg)](https://github.com/RediSearch/ftsb/releases/latest)
[![Codecov](https://codecov.io/gh/RediSearch/ftsb/branch/master/graph/badge.svg)](https://codecov.io/gh/RediSearch/ftsb)
[![Go Report Card](https://goreportcard.com/badge/github.com/RediSearch/ftsb)](https://goreportcard.com/report/github.com/RediSearch/ftsb)
[![GoDoc](https://godoc.org/github.com/RediSearch/ftsb?status.svg)](https://godoc.org/github.com/RediSearch/ftsb)

# Full-Text Search Benchmark (FTSB)
 [![Forum](https://img.shields.io/badge/Forum-RediSearch-blue)](https://forum.redislabs.com/c/modules/redisearch/) 
[![Discord](https://img.shields.io/discord/697882427875393627?style=flat-square)](https://discord.gg/xTbqgTB)

This repo contains code for benchmarking full text search databases,
including RediSearch.
This code is based on a fork of work initially made public by TSBS
at https://github.com/timescale/tsbs.



## Overview
The Full-Text Search Benchmark (FTSB) is a collection of Python and Go programs that are used to generate datasets (Python) and then benchmark(Go) read and write performance of various databases. The intent is to make the FTSB extensible so that a variety of use cases (e.g., ecommerce, jsondata, logs, etc.), query types, and databases can be included and benchmarked.
To this end, we hope to help SAs, and prospective database administrators find the best database for their needs and their workloads.


## What the FTSB tests

FTSB is used to benchmark bulk load performance and query execution performance. To accomplish this in a fair way, the data to be inserted and the queries to run are always pre-generated and native Go clients are used wherever possible to connect to each database.

## Current databases supported

+ RediSearch

### Current use cases

Currently, FTSB supports three use cases: 
- **nyc_taxis** [[details here](docs/nyc_taxis-benchmark/description.md)]. This benchmark focusses on write performance, making usage of TLC Trip Record Data that contains the rides that have been performed in yellow cab taxis in New York in 2015. The benchmark loads over 12M documents.

- **enwiki-abstract** [[details here](docs/enwiki-abstract-benchmark/description.md)], from English-language [Wikipedia:Database](https://en.wikipedia.org/wiki/Wikipedia:Database_download) page abstracts. This use case generates 3 `TEXT` fields per document, and focusses on full text queries performance.

- **enwiki-pages** [[details here](docs/enwiki-pages-benchmark/description.md)], from English-language [Wikipedia:Database](https://en.wikipedia.org/wiki/Wikipedia:Database_download) last page revisions, containing processed metadata extracted from the full Wikipedia XML dumppage abstracts. This use case generates 3 `TEXT` fields per document, and focuses on full text queries performance.

- **ecommerce-inventory** [[details here](docs/ecommerce-inventory-benchmark/description.md)], from a base dataset of [10K fashion products on Amazon.com](https://data.world/promptcloud/fashion-products-on-amazon-com/workspace/file?filename=amazon_co-ecommerce_sample.csv) which are then multiplexed by categories, sellers, and countries to produce larger datasets (> 1M documents). This benchmark focuses on updates and aggregate performance, splitting into Reads (FT.AGGREGATE), Cursor Reads (FT.CURSOR), and Updates (FT.ADD) the performance numbers. 
The use case generates an index with 10 `TAG` fields (3 sortable and 1 non indexed), and 16 `NUMERIC` sortable non indexed fields per document.
The aggregate queries are designed to be extremely costly both on computation and network TX, given that each query aggregates and filters a large portion of the dataset while additionally loading 21 fields. Both the update and read rates can be adjusted.



### Installation

#### Download Standalone binaries ( no Golang needed )

If you don't have go on your machine and just want to use the produced binaries you can download the following prebuilt bins:

https://github.com/RediSearch/ftsb/releases/latest

| OS | Arch | Link |
| :---         |     :---:      |          ---: |
| Linux   | amd64  (64-bit X86)     | [ftsb_redisearch-linux-amd64](https://github.com/RediSearch/ftsb/releases/latest/download/ftsb_redisearch-linux-amd64.tar.gz)    |
| Linux   | arm64 (64-bit ARM)     | [ftsb_redisearch-linux-arm64](https://github.com/RediSearch/ftsb/releases/latest/download/ftsb_redisearch-linux-arm64.tar.gz)    |
| Darwin   | amd64  (64-bit X86)     | [ftsb_redisearch-darwin-amd64](https://github.com/RediSearch/ftsb/releases/latest/download/ftsb_redisearch-darwin-amd64.tar.gz)    |
| Darwin   | arm64 (64-bit ARM)     | [ftsb_redisearch-darwin-arm64](https://github.com/RediSearch/ftsb/releases/latest/download/ftsb_redisearch-darwin-arm64.tar.gz)    |

Here's how bash script to download and try it:

```bash
wget -c https://github.com/RediSearch/ftsb/releases/latest/download/ftsb_redisearch-$(uname -mrs | awk '{ print tolower($1) }')-$(dpkg --print-architecture).tar.gz -O - | tar -xz

# give it a try
./ftsb_redisearch --help
```


#### Installation in a Golang env

To install the benchmark utility with a Go Env do as follow:

```bash
# Fetch FTSB and its dependencies
go get github.com/RediSearch/ftsb
cd $GOPATH/src/github.com/RediSearch/ftsb

# Install desired binaries. At a minimum this includes ftsb_redisearch binary:
make

# give it a try
./bin/ftsb_redisearch --help
```



## How to use it?

Using FTSB for benchmarking involves 2 phases: data and query generation, and query execution.


### Data and query generation ( single time step )

So that benchmarking results are not affected by generating data or queries on-the-fly, with FTSB you generate the data and queries you want to benchmark first, and then you can (re-)use it as input to the benchmarking phase. You can either use one of the pre-baked benchmark suites or develop one of your own. The requirement is that of the generated benchmark input file(s) they all respect the following:

- CSV format, with one command per line. 

- On each line, the first three columns are related to the query type (READ, WRITE, UPDATE, DELETE, SETUP_WRITE), query group ( any unique identifier you like. example Q1 ), and key position. 

- The columns >3 are the command and command arguments themselves, with one column per command argument. 

Here is an example of a CSV line:
```
WRITE,U1,2,FT.ADD,idx,doc1,1.0,FIELDS,title,hello world
```
which will translate to the following command being issued:
```
FT.ADD idx doc1 1.0 FIELDS title "hello world"
```
The following links deep dive on:

- Generating inputs from pre-baked benchmark suites (ecommerce-inventory , enwiki-abstract , enwiki-pages) 

- Generating your own use cases 

Apart from the CSV files, and not mandatory, there is a benchmark suite specification that enables you to describe in detail the benchmark, what key metrics it provides, and how to automatically run more complex suites (with several steps, etc… ). This is not mandatory and for a simple benchmark, you just need to feed the CSV file as input. 


### Query execution ( benchmarking )

So that benchmarking results are not affected by generating data or queries on-the-fly, you are always required to feed an input file to the benchmark runner that respects the previous specification format. The overall idea is that the benchmark runner only concerns himself on executing the queries as fast as possible while enabling client runtime variations that influence performance ( and are not related to the use-case himself ) like, command pipelining ( auto pipelining based on time or number of commands ), cluster support, number of concurrent clients, rate limiting ( to find sustainable throughputs ), etc… 

Running a benchmark is as simple as feeding an input file to the DB benchmark runner ( in this case ftsb_redisearch ):

```bash

ftsb_redisearch --file ecommerce-inventory.redisearch.commands.BENCH.csv
```


The resulting stdout output will look similar to this:

```bash
$ ftsb_redisearch --file ecommerce-inventory.redisearch.commands.BENCH.csv 
    setup writes/sec          writes/sec         updates/sec           reads/sec    cursor reads/sec         deletes/sec     current ops/sec           total ops             TX BW/sRX BW/s
          0 (0.000)           0 (0.000)        1571 (2.623)         288 (7.451)           0 (0.000)           0 (0.000)        1859 (3.713)                1860             3.1KB/s  1.4MB/s
          0 (0.000)           0 (0.000)        1692 (2.627)         287 (7.071)           0 (0.000)           0 (0.000)        1979 (3.597)                3839             3.3KB/s  1.4MB/s
          0 (0.000)           0 (0.000)        1571 (2.761)         293 (7.087)           0 (0.000)           0 (0.000)        1864 (3.679)                5703             3.1KB/s  1.4MB/s
          0 (0.000)           0 (0.000)        1541 (2.983)         280 (7.087)           0 (0.000)           0 (0.000)        1821 (3.739)                7524             3.1KB/s  1.4MB/s
          0 (0.000)           0 (0.000)        1441 (2.989)         255 (7.375)           0 (0.000)           0 (0.000)        1696 (3.773)                9220             2.8KB/s  1.3MB/s

Summary:
Issued 9885 Commands in 5.455sec with 8 workers
        Overall stats:
        - Total 1812 ops/sec                    q50 lat 3.819 ms
        - Setup Writes 0 ops/sec                q50 lat 0.000 ms
        - Writes 0 ops/sec                      q50 lat 0.000 ms
        - Reads 276 ops/sec                     q50 lat 7.531 ms
        - Cursor Reads 0 ops/sec                q50 lat 0.000 ms
        - Updates 1536 ops/sec                  q50 lat 3.117 ms
        - Deletes 0 ops/sec                     q50 lat 0.000 ms
        Overall TX Byte Rate: 3KB/sec
        Overall RX Byte Rate: 1.4MB/sec
```


Apart from the input file, you should also always specify the name of JSON output file to output benchmark results, in order to do more complex analysis or store the results. Here is the full list of supported options:

```bash
$ ./ftsb_redisearch --help
Usage of ./bin/ftsb_redisearch:
  -a string
        Password for Redis Auth.
  -cluster-mode
        If set to true, it will run the client in cluster mode.
  -continue-on-error
        If set to true, it will continue the benchmark and print the error message to stderr.
  -debug int
        Debug printing (choices: 0, 1, 2). (default 0)
  -do-benchmark
        Whether to write databuild. Set this flag to false to check input read speed. (default true)
  -host string
        The host:port for Redis connection (default "localhost:6379")
  -input string
        File name to read databuild from
  -json-out-file string
        Name of json output file to output benchmark results. If not set, will not print to json.
  -max-rps uint
        enable limiting the rate of queries per second, 0 = no limit. By default no limit is specified and the binaries will stress the DB up to the maximum. A normal "modus operandi" would be to initially stress the system ( no limit on RPS) and afterwards that we know the limit vary with lower rps configurations.
  -metadata-string string
        Metadata string to add to json-out-file. If -json-out-file is not set, will not use this option.
  -pipeline int
        Pipeline <numreq> requests. Default 1 (no pipeline). (default 1)
  -reporting-period duration
        Period to report write stats (default 1s)
  -requests uint
        Number of total requests to issue (0 = all of the present in input file).
  -workers uint
        Number of parallel clients inserting (default 8)
```
