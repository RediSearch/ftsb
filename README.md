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
best database for their needs and their workloads.   
Further, if you
are the developer of a Full-Text Search database and want to include your
database in the FTSB, feel free to open a pull request to add it!

## Current use cases

Currently, FTSB supports one use case -- enwiki-abstract, From English-language [Wikipedia:Database](https://en.wikipedia.org/wiki/Wikipedia:Database_download) page abstracts. This use case generates
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
the `ftsb_generate_data` tool. The following example outputs the generated queries to a file named `enwiki-latest-abstract1.gz` in directory `/tmp`:
```bash
$ curl -O https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-abstract1.xml.gz
$ gunzip enwiki-latest-abstract1.xml.gz /tmp/enwiki-latest-abstract1.xml
$ ftsb_generate_data -input-file /tmp/enwiki-latest-abstract1.xml \
     -format="redisearch" \
    | gzip > /tmp/ftsb_generate_data-enwiki-latest-abstract1-redisearch.gz 

# Each additional database would be a separate call.
```
_Note: We pipe the output to gzip to reduce on-disk space._


#### Query generation

Variables needed:
1. the same use case
1. the number of queries to generate. E.g., `1000`
1. and the type of query you'd like to generate. E.g., `single-word-query`

For the last step there are numerous queries to choose from, which are
listed in [Appendix I](#appendix-i-query-types). 

For generating just one set of queries for a given type:
```bash
$ ftsb_generate_queries -query-type="simple-2word-query" \
    -queries 100000 -input-file /tmp/enwiki-latest-abstract1.xml \
     -output-file /tmp/redisearch-queries-enwiki-latest-abstract1-simple-2word-query-100K-queries-1-0-0
```

In debug mode 0, only the summary of query generation will be printed:
```text
using random seed 1234
Reading /tmp/enwiki-latest-abstract1.xml
RediSearch Simple 2 Word Query - English-language Wikipedia:Database page abstracts (random words): 100000 queries
```


In debug mode 2, the full info of the generated queries will will be printed:
```text
using random seed 1234
Reading /tmp/enwiki-latest-abstract1.xml
RediSearch Simple 2 Word Query - English-language Wikipedia:Database page abstracts (random in set words). Used words "their based"
RediSearch Simple 2 Word Query - English-language Wikipedia:Database page abstracts (random in set words). Used words "reflection of"
RediSearch Simple 2 Word Query - English-language Wikipedia:Database page abstracts (random in set words). Used words "In Thetis"
RediSearch Simple 2 Word Query - English-language Wikipedia:Database page abstracts (random in set words). Used words "Abraham Lincoln"
RediSearch Simple 2 Word Query - English-language Wikipedia:Database page abstracts (random in set words). Used words "deathplaceEuboea Macedonian"
RediSearch Simple 2 Word Query - English-language Wikipedia:Database page abstracts (random in set words). Used words "An American"
(...)
RediSearch Simple 2 Word Query - English-language Wikipedia:Database page abstracts (random words): 100000 queries
```

### Benchmarking insert/write performance


FTSB measures insert/write performance by taking the data generated in
the previous step and using it as input to a database-specific command
line program.  
Each loader does share some common flags -- e.g., batch size (number of readings inserted
together), workers (number of concurrently inserting clients), connection
details (host & ports), etc -- but they also have database-specific tuning
flags. To find the flags for a particular database, use the `-help` flag
(e.g., `ftsb_load_redisearch -help`).

So for loading documents into Redis using RediSearch use:
```bash
# flush the database
$ redis-cli flushall 

# create the index
$ redis-cli ft.create idx1 SCHEMA \
      TITLE TEXT WEIGHT 5 \
      URL TEXT WEIGHT 5 \ 
      ABSTRACT TEXT WEIGHT 1

# Will insert using 2 clients, batch sizes of 10k, from a file
# named `enwiki-latest-abstract1.gz` in directory `/tmp`
# with pipeline of 100
$ cat /tmp/ftsb_generate_data-enwiki-latest-abstract1-redisearch.gz \
      | gunzip \
      | ./ftsb_load_redisearch -workers 2 -reporting-period 1s \
       -batch-size 1000 -pipeline 10
```

---

By default, statistics about the load performance are printed every 10s,
and when the full dataset is loaded the looks like this:
```text
time,per. docs/s,docs total,overall docs/s
# ...
1563638164,12029.88,5.280000E+05,13199.17
1563638165,11986.80,5.400000E+05,13169.57
1563638166,10026.50,5.500000E+05,13094.94
1563638167,9965.59,5.600000E+05,13021.92
1563638168,12008.42,5.720000E+05,12998.90
1563638169,11994.32,5.840000E+05,12976.57

Summary:
loaded 587640 Documents in 45.090sec with 4 workers (mean rate 13032.72 metrics/sec)
```

All but the last two lines contain the data in CSV format, with column names in the header. Those column names correspond to:
* timestamp,
* inserted documents per second in the period,
* total documents inserted,
* overall documents per second,

The last line is a summary of how many documents were inserted, the wall time it took, and the average rate
of insertion.


### Benchmarking query execution performance

To measure query execution performance in FTSB, you first need to load
the data using the previous section and generate the queries as
described earlier. Once the data is loaded and the queries are generated,
just use the corresponding `ftsb_run_queries_` binary for the database
being tested:
```bash
$ ftsb_run_queries_redisearch \
       -file /tmp/redisearch-queries-enwiki-latest-abstract1-simple-2word-query-100K-queries-1-0-0 \
       -max-queries 100000 -workers 16 -print-interval 20000 
```

You can change the value of the `--workers` flag to
control the level of parallel queries run at the same time. The
resulting output will look similar to this:
```text
after 20000 queries with 16 workers:
RediSearch Simple 2 Word Query - English-language Wikipedia:Database page abstracts (random words):
min:     0.20ms, med:     2.55ms, mean:     3.96ms, max:   37.85ms, stddev:     3.86ms, sum:  79.2sec, count: 20000
all queries                                                                                       :
min:     0.20ms, med:     2.55ms, mean:     3.96ms, max:   37.85ms, stddev:     3.86ms, sum:  79.2sec, count: 20000

after 40000 queries with 16 workers:
RediSearch Simple 2 Word Query - English-language Wikipedia:Database page abstracts (random words):
min:     0.12ms, med:     2.11ms, mean:     3.47ms, max:   37.85ms, stddev:     3.79ms, sum: 138.8sec, count: 40000
all queries                                                                                       :
min:     0.12ms, med:     2.11ms, mean:     3.47ms, max:   37.85ms, stddev:     3.79ms, sum: 138.8sec, count: 40000

after 60000 queries with 16 workers:
RediSearch Simple 2 Word Query - English-language Wikipedia:Database page abstracts (random words):
min:     0.11ms, med:     1.90ms, mean:     3.22ms, max:   37.85ms, stddev:     3.63ms, sum: 193.1sec, count: 60000
all queries                                                                                       :
min:     0.11ms, med:     1.90ms, mean:     3.22ms, max:   37.85ms, stddev:     3.63ms, sum: 193.1sec, count: 60000

after 80000 queries with 16 workers:
RediSearch Simple 2 Word Query - English-language Wikipedia:Database page abstracts (random words):
min:     0.11ms, med:     2.02ms, mean:     3.37ms, max:   37.85ms, stddev:     3.70ms, sum: 269.9sec, count: 80000
all queries                                                                                       :
min:     0.11ms, med:     2.02ms, mean:     3.37ms, max:   37.85ms, stddev:     3.70ms, sum: 269.9sec, count: 80000

run complete after 100000 queries with 16 workers:
RediSearch Simple 2 Word Query - English-language Wikipedia:Database page abstracts (random words):
min:     0.11ms, med:     2.06ms, mean:     3.47ms, max:   39.74ms, stddev:     3.81ms, sum: 347.3sec, count: 100000
all queries                                                                                       :
min:     0.11ms, med:     2.06ms, mean:     3.47ms, max:   39.74ms, stddev:     3.81ms, sum: 347.3sec, count: 100000
wall clock time: 40.529041sec
```


## Appendix I: Query types <a name="appendix-i-query-types"></a>

### English-language [Wikipedia:Database](https://en.wikipedia.org/wiki/Wikipedia:Database_download) page abstracts.
|Query type|Description|Status|
|:---|:---|:---|
|simple-1word-query| Simple 1 Word Query | not ready
|simple-2word-query| Simple 2 Word Query| not ready (WIP)
|exact-3word-match| Exact 3 Word Match| not ready
|autocomplete-1100-top3| Autocomplete -1100 Top 2-3 Letter Prefixes| not ready