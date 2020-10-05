## English-language [Wikipedia:Database](https://en.wikipedia.org/wiki/Wikipedia:Database_download) page abstracts


### Example Document
This use case generates 5.9 million docs, with 3 Text fields per document. 
On average each added document will have a size of 227 bytes.

(prior field tokenization)
```
{
'title': 'Wikipedia: Politics of the Democratic Republic of the Congo',
'url': 'https://en.wikipedia.org/wiki/Politics_of_the_Democratic_Republic_of_the_Congo',
'abstract': 'Politics of the Democratic Republic of Congo take place in a framework of a republic in transition from a civil war to a semi-presidential republic.'
}
```

## Query types

### Full text search queries
|Query type|Description|Example|Status|
|:---|:---|:---|:---|
|simple-1word-query| Simple 1 Word Query | `Abraham` | :heavy_check_mark:
|2word-union-query| 2 Word Union Query | `Abraham Lincoln` | :heavy_check_mark:
|2word-intersection-query| 2 Word Intersection Query| `Abraham`&#124;`Lincoln` | :heavy_check_mark:
|exact-3word-match| Exact 3 Word Match| `"President Abraham Lincoln"` |:heavy_multiplication_x:
|autocomplete-1100-top3| Autocomplete -1100 Top 2-3 Letter Prefixes|  | :heavy_multiplication_x:
|2field-2word-intersection-query| 2 Fields, one word each, Intersection query | `@text_field1: text_value1 @text_field2: text_value2` | :heavy_multiplication_x:
|2field-1word-intersection-1numeric-range-query| 2 Fields, one text and another numeric, Intersection and numeric range query | `@text_field: text_value @numeric_field:[{min} {max}]` |:heavy_multiplication_x:

### Spell Check queries

Performs spelling correction on a query, returning suggestions for misspelled terms.
To simmulate misspelled terms, for each word a deterministic random number of edits in the range 0..Min(word.length/2 , 4) is chosen. 

For each edit a random type of edit (delete, insert random char, replace with random char, switch adjacent chars).

|Query type|Description|Example|Status|
|:---|:---|:---|:---|
| simple-1word-spellcheck | Simple 1 Word Spell Check Query | `FT.SPELLCHECK {index} reids DISTANCE 1` | :heavy_check_mark:

### Autocomplete queries
|Query type|Description|Example|Status|
|:---|:---|:---|:---|
| |  | `` | :heavy_multiplication_x:


### Aggregate queries

Aggregations are a way to process the results of a search query, group, sort and transform them - and extract analytic insights from them. Much like aggregation queries in other databases and search engines, they can be used to create analytics reports, or perform Faceted Search style queries. 

|Query type|Description|Clauses included|Status|
|:---|:---|:---|:---|
| |  | `` | :heavy_multiplication_x:

### Synonym queries
|Query type|Description|Example|Status|
|:---|:---|:---|:---|
| |  | `` | :heavy_multiplication_x:


## How to benchmark

Using FTSB for benchmarking involves 2 phases: data and query generation, and query execution.  
The following steps focus on how to retrieve the data and generate the commands for the nyc_taxis use case. 

## Generating the dataset

To generate the required dataset command file issue:
```
cd $GOPATH/src/github.com/RediSearch/ftsb/scripts/datagen_redisearch/enwiki_abstract
python3 ftsb_generate_enwiki_abstract.py 
```

### Index properties
The use case generates an secondary index with 3 fields per document:
- 3 TEXT sortable fields.

## Running the benchmark

Assuming you have `redisbench-admin` and `ftsb_redisearch` installed, for the default dataset, run:

```
redisbench-admin run \
     --repetitions 3 \
     --benchmark-config-file  https://s3.amazonaws.com/benchmarks.redislabs/redisearch/datasets/enwiki_abstract-hashes/enwiki_abstract-hashes.redisearch.cfg.json
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
