## NYC taxis use case

This benchmark focus himself on write performance, making usage of TLC Trip Record Data that contains the rides that have been performed in yellow taxis in New York in 2015.
On total, the benchmark loads >12M documents like the following one:

### Example Document
On average each added document will have a size of 500 bytes.
```
{
  "total_amount": 6.3,
  "improvement_surcharge": 0.3,
  "pickup_location_long_lat": "-73.92259216308594,40.7545280456543",
  "pickup_datetime": "2015-01-01 00:34:42",
  "trip_type": "1",
  "dropoff_datetime": "2015-01-01 00:38:34",
  "rate_code_id": "1",
  "tolls_amount": 0.0,
  "dropoff_location_long_lat": "-73.91363525390625,40.76552200317383",
  "passenger_count": 1,
  "fare_amount": 5.0,
  "extra": 0.5,
  "trip_distance": 0.88,
  "tip_amount": 0.0,
  "store_and_fwd_flag": "N",
  "payment_type": "2",
  "mta_tax": 0.5,
  "vendor_id": "2"
}
```
Depending on the benchmark variation it uses either `FT.ADD` or `HSET` commands. By default HSET will be used.

## How to benchmark

Using FTSB for benchmarking involves 2 phases: data and query generation, and query execution.  
The following steps focus on how to retrieve the data and generate the commands for the nyc_taxis use case. 

## Generating the dataset
The original dataset is present in https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page but the generator will automatically download the required data.

To generate the required dataset command file issue:
```
cd $GOPATH/src/github.com/RediSearch/ftsb/scripts/datagen_redisearch/nyc_taxis
python3 ftsb_generate_nyc_taxis.py 
```

This will download 1 to 12 files ( depending on the start and end date ) for a temporary folder and preprocess them to be ingested. 
On total you should expected a large `nyc_taxis.redisearch.commands.ALL.tar.gz` file to be generated with >12M commands to be issued to the DB, alongside it's config json `nyc_taxis.redisearch.cfg.json`.


### FT.ADD variation
To generate the FT.ADD variations you just need to include the `use-ftadd` flag, as follow: 
```
python3 ftsb_generate_nyc_taxis.py --use-ftadd --test-name nyc_taxis-ftadd
```

### Index properties
The use case generates an secondary index with 18 fields per document:
- 5 TAG sortable fields.
- 9 NUMERIC sortable fields.
- 2 TEXT sortable fields.
- 2 GEO sortable fields.


## Running the benchmark

Assuming you have `redisbench-admin` and `ftsb_redisearch` installed, for the default dataset with >12M documents, run:

### HSET variation
```
redisbench-admin run \
     --repetitions 3 \
     --benchmark-config-file https://s3.amazonaws.com/benchmarks.redislabs/redisearch/datasets/nyc_taxis-hashes/nyc_taxis-hashes.redisearch.cfg.json
```

### FT.ADD variation
```
redisbench-admin run \
     --repetitions 3 \
     --benchmark-config-file https://s3.amazonaws.com/benchmarks.redislabs/redisearch/datasets/nyc_taxis-ft.add/nyc_taxis-ft.add.redisearch.cfg.json
```

### Key Metrics:
After running the benchmark you should have a result json file generated, containing key information about the benchmark run(s).
Focusing specifically on this benchmark the following metrics should be taken into account and will be used to automatically choose the best run and assess results variance, ordered by the following priority ( in case of results comparison ):

| Metric Family | Metric Name            | Unit         | Comparison mode  |
|---------------|------------------------|--------------|------------------|
| Throughput    | Overall Ingestion rate | docs/sec     | higher is better |
| Latency       | Overall ingestion p50  | milliseconds | lower is better  |
