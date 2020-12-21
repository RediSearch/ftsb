## English-language [Wikipedia:Database](https://en.wikipedia.org/wiki/Wikipedia:Database_download) last page revisions


### Example Document
This use case generates 1 million docs, with 3 TEXT fields (all sortable), 1 sortable TAG field, and 1 sortable NUMERIC fields per document. 

We’ve targeted large documents, with an average size of 45KB, and single documents that can reach 300KB.

## Query types

### Full text search queries
|Query type|Description|Example|Status|
|:---|:---|:---|:---|
|simple-1word-query| Simple 1 Word Query | `@timestamp:[1604149522.5400035 1604270992.0] Abraham` | :heavy_check_mark:
|2word-union-query| 2 Word Union Query | `@timestamp:[1604149522.5400035 1604270992.0] Abraham Lincoln` | :heavy_check_mark:
|2word-intersection-query| 2 Word Intersection Query| `@timestamp:[1604149522.5400035 1604270992.0] Abraham`&#124;`Lincoln` | :heavy_check_mark:


## How to benchmark

Using FTSB for benchmarking involves 2 phases: data and query generation, and query execution.  
The following steps focus on how to retrieve the data and generate the commands for the nyc_taxis use case. 

## Generating the dataset

To generate the required dataset command file issue:
```
cd $GOPATH/src/github.com/RediSearch/ftsb/scripts/datagen_redisearch/enwiki_pages
python3 ftsb_generate_enwiki_pages.py 
```

### Index properties
The use case generates an secondary index with with 3 TEXT fields (all sortable), 1 sortable TAG field, and 1 sortable NUMERIC fields per document.

## Running the benchmark

Assuming you have `redisbench-admin` and `ftsb_redisearch` installed, for the default dataset, run:

```
redisbench-admin run \
     --repetitions 3 \
     --benchmark-config-file  https://s3.amazonaws.com/benchmarks.redislabs/redisearch/datasets/enwiki_pages-hashes/enwiki_pages-hashes.redisearch.cfg.json
```


### Key Metrics:
After running the benchmark you should have a result json file generated, containing key information about the benchmark run(s).
Focusing specifically on this benchmark the following metrics should be taken into account and will be used to automatically choose the best run and assess results variance, ordered by the following priority ( in case of results comparison ):

#### Setup step key metrics
| Metric Family | Metric Name            | Unit         | Comparison mode  |
|---------------|------------------------|--------------|------------------|
| Throughput    | Overall Ingestion rate | docs/sec     | higher is better |
| Latency       | Overall ingestion p50  | milliseconds | lower is better  |

#### Benchmark step key metrics
| Metric Family | Metric Name            | Unit         | Comparison mode  |
|---------------|------------------------|--------------|------------------|
| Throughput | Overall Updates and Aggregates query rate | docs/sec | Higher is better | 
| Latency | Overall Updates and Aggregates query q50 latency | milliseconds | Lower is better | 
