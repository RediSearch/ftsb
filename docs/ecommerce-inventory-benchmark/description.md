## Ecommerce inventory use case

From a base dataset of 10K fashion products on Amazon.com which are then multiplexed by categories, sellers, and countries to produce larger datasets > 1M docs, this benchmark focuses on updates and aggregate performance, splitting into Reads (FT.AGGREGATE), and Updates (FT.ADD) the performance numbers. 

The aggregate queries are designed to be extremely costly both on computation and network TX, given that on each query we're aggregating and filtering over a large portion of the dataset while additionally loading 21 fields. 

Both the update and read rates can be adjusted.  For a sample of each benchmark query see the Sample benchmark queries section.

### Example Document

```
{
 "market" : "US",
 "nodeId" : "1",
 "skuId" : "eac7efa5dbd3d667f26eb3d3ab504464",
 "onhand" : "12433",
 "onhandLastUpdatedTimestamp" : "1600104589",
 "allocated" : "10520",
 "allocatedLastUpdatedTimestamp" : "1600122420",
 "reserved" : "5364",
 "reservedLastUpdatedTimestamp" : "1600117671",
 "storeAllocated" : "9361",
 "storeAllocatedLastUpdatedTimestamp" : "1600170143",
 "transferAllocated" : "53579",
 "transferAllocatedLastUpdatedTimestamp" : "1600106905",
 "storeReserved" : "20087",
 "storeReservedLastUpdatedTimestamp" : "1600147472",
 "confirmedQuantity" : "15220",
 "standardSafetyStock" : "21812",
 "bopisSafetyStock" : "27806",
 "virtualHold" : "13166",
 "availableToSource" : "true",
 "standardAvailableToPromise" : "true",
 "bopisAvailableToPromise" : "true",
 "nodeType" : "store",
 "brand" : "Hornby",
 "onHold" : "false",
 "exclusionType" : "false",
}
```

## Query types

### Aggregate queries

Aggregations are a way to process the results of a search query, group, sort and transform them - and extract analytic insights from them. Much like aggregation queries in other databases and search engines, they can be used to create analytics reports, or perform Faceted Search style queries. 

|Query type|Description|Clauses included|Status|
|:---|:---|:---|:---|
|q1| Aggregate across 21 fields | <See sample aggregate query> | :heavy_check_mark:

#### Sample q1
```
"FT.AGGREGATE" "inventory" "@market:{US} @skuId:{eee6f264563e61d690ceabb6a8bb28d6|2fe6e513acb7ace5aa7b89229083177b|d96328b0c08be37483282fed691e4d50|101778de5f9a4c8f134dd9abe60438c7|645963128e1f3df9fa6d6b0532108692|8879776477df76e540c82ca36ebd8b9e|35f998be32160c459d1d7b299e1cd7ec|f15ad197f43696982851a459ea1d98f8|58a29f6572df84c2bde26e473cc6fb0f|25d789a3b0651447a5a292670c971c21|0450c568b52f0e654616eef013d04746|052b9bc93b18747309483128313a2b76|72eeb30838e409980c344ed8608a7768|2953bdb96b2450c28c4ded0f9d30c2fc|d9f74437ffff1b055e79ae86dae2ff49|995777a914a1edb958a0da25b145643b|9aa711b367533c012cb110d7ebca844b|4ea4a1cf2b2136a8ba1c31f2708d8962|a015329569179c7775dcf5ce263330bb|f25ec41379c99e5e4fecfd967af82847|808d68823c96d75ee1ae869b6bb98133|556ac5522c39f050bd8a20767d8e1a4b|65beb05bc8e212af16a20a6970ac7b0c|8aa01bfb947d2aa80e65bc2dc34ec3da|fe62d40eda72db44d39d74f94a1168ff|2131da14a8f4f6d75270bdeb29b926cc|7474cf8f2da17c55263b4b192f18d77b|3e232649e15d7bf87055d30c372e3c8a|6ff572b7a995330f6bfd41a7d139da17|a50caf5a58d8a62571e2186c22c14654|1c2fb56fa30f483e270cb8a5441f7e8b|4b7bb58d0635bbfee6997658db1c1039|e7ac7e1effaa54cc409a09b711659416|42fccdc1368987b8b10486d060504d54|a801a071909b6561962df79b40ef28b0|ffee9727cfb5f53e12dd9d9fca0cf8aa|3a0fec0884138388e4c15ae8efca8d15|bf1143be4c3665e7ef1923428ca96cb4|74d2f09e30529fe19645cdaa86d5a79b|bd9749a58b96ebe1b4e9d66fad08ae7d|bdf9d6874160d07df78424271bdea6d7|d19c9fca1b537ed5bfe6f3ddd1ecd4fb|f157293c89ba2e116475d555992c778a|0eb15184a4050bbfa3bdc0c8789e57bf|25cdbda21e0f7e165c0136a7034e9304|0e3152aba3f7eb9cf2a84c2a0b56150c|76673c4eae2d8ecb09bdb9c880de40ab|d00536420b49a24024d1478353930c08|135c298448acf57a99fb36c785cb812c|1f8bbd31df2ceab3b67c9ac66e4729d3|3b78bdce914b8767a80203adad2f3ec7|8e6623cff18270ec508df41c93cc112a|be5728309319c53188ef6a40e7fe9bbc|808861a3a7355a080e0e1ad3cd51ba3f|8430d6b58ced27973b22c318d9557139|b4cb7a143965035274bb0b7cdf245bf7|27b7e4f72488d6345bdcac8d2a03dbb6|29f0058f73d4aff6008ebb434ca65433|dfbc15b579cfb269c07ecaab8d3c2c6a|227db01ce18777bbf75dd39677bcb38a|b46bd73106737d8c2804d34aa04edcaf|92df5b755346feb7f3b0a93498e363e1|5e1126991cc8cb793549f3e0f199e7c1|9e9c317f3c2e4337d0f85f0ecb0c8a0d|a3b2210b1e090d68dc883ef120a7dcd3|eff388ea4bfddbbdc1b86cd5738f66e7|f69f3a25f851f3f56acfbc1bc2d5029e|dda2d6752683937a8c3cd338ccd034a7|42b2dc99bba449644798be12c197b959|342d2b01ed22bc6a889a4b33949d516f|87132b02061eac84e9261d9635996f8d|9cd7af6fbba4d82159c61183ecc3d8c4|e17d1b120237c88edfa1bfbb4e72cd09|95534049f7431eea318a6018bc1eff3f|4ea4a1cf2b2136a8ba1c31f2708d8962|c89a713edaf634e80ab5fcd349c0035d|7d47627e91b6fa1d313da8151e9d8ffd|7ad3004492c90f1cff628097ac52303b|a20b236a5a7e92515ba07d91bf32c0ab|227b29472146ddbe9cabd9d98ad453c5|a9a5ca5c6241deb0c809d006ee3ae26b|eeadf21a379a4c1d47eaf674b5ba321b|c94992bd3a1075962c5c566187826ce5|65a69f7accc778a9f7d576f45701d8ac|e67c3eb27ed74448111e9c8686606ddb|2657a9d69f0a695f23f6e227dc559eb8|76c13e372a882b41dd5232ef198bf886|cf8cbdc12ebc3f75dc145f2b665b429b|ee68a4d2e5ea7b6cc03a2634bac2f58e|a6ba5e11b1d3414e5d624448f2fc2e1f|59045d3273dd03bef983f4f0eefcb31b|c98df0aee64e88a3a2cb9a9b530fe72e|89ea26d69fe164bc4c96c0fd3a61b84c|555c900756b846482ed5821ee37e412d|1affc5987967a343ecf4aaff9e3d43c0|a904949d92b880508aa0ca19827db3a7|bae24935ccf8743d4e2a77a6d1df04ae|584acaeb3b2b569637f2dab72eb39b23|416e1c8974407729c41983cbfe08d3a1|33e64eaacbfc53e484f46fd4f79d3c5e} @nodeId:{482|1630|1138|964|92|587|1354|920|967|197|1312|642|358|952|1117|1211|1308|344|1532|1280|1109|1663|638|769|455|1032|233|527|417|127|1521|1447|614|208|329|1009|1632|1005|1260|1581|652|1012|1458|366|1438|1154|763|1017|248|1639|196|1073|39|297|1692|1304|1598|451|708|570|21|30|1344|1209|492|1316|524|508|1413|1499|538|1698|218|147|1184|277|5|1182|955|774|417|1378|845|835|475|667|653|288|1042|852|1059|1568|674|1162|917|942|1147|976|1306|325}" "LOAD" "21" "@market" "@skuId" "@nodeId" "@brand" "@nodeType" "@onhand" "@allocated" "@confirmedQuantity" "@reserved" "@virtualHold" "@availableToSource" "@standardAvailableToPromise" "@bopisAvailableToPromise" "@storeAllocated" "@bopisSafetyStock" "@transferAllocated" "@standardSafetyStock" "@storeReserved" "@availableToSource" "@exclusionType" "@onHold" "WITHCURSOR" "COUNT" "500"
```

## How to benchmark

Using FTSB for benchmarking involves 2 phases: data and query generation, and query execution.  
The following steps focus on how to retrieve the data and generate the commands for the nyc_taxis use case. 

## Generating the dataset

To generate the required dataset command file issue:
```
cd $GOPATH/src/github.com/RediSearch/ftsb/scripts/datagen_redisearch/enwiki_abstract
python3 ftsb_generate_enwiki-abstract.py 
```

### Index properties
The use case generates an secondary index with 3 fields per document:
- 3 TEXT sortable fields.

## Running the benchmark

Assuming you have `redisbench-admin` and `ftsb_redisearch` installed, for the default dataset, run:

```
redisbench-admin run \
     --repetitions 3 \
     --benchmark-config-file https://s3.amazonaws.com/benchmarks.redislabs/redisearch/datasets/enwiki-abstract/enwiki-abstract.redisearch.cfg.json
```

### Key Metrics:
After running the benchmark you should have a result json file generated, containing key information about the benchmark run(s).
Focusing specifically on this benchmark the following metrics should be taken into account and will be used to automatically choose the best run and assess results variance, ordered by the following priority ( in case of results comparison ):

#### Setup step key metrics
| Metric Family | Metric Name            | Unit         | Comparison mode  |
|---------------|------------------------|--------------|------------------|
| Throughput    | Overall Ingestion rate | docs/sec     | higher is better |
| Latency       | Overall ingestion p50  | milliseconds | lower is better  |

#### Benchmarking step key metrics
| Metric Family | Metric Name            | Unit         | Comparison mode  |
|---------------|------------------------|--------------|------------------|
| Throughput | Overall Updates and Aggregates query rate | docs/sec | Higher is better | 
| Latency | Overall Updates and Aggregates query q50 latency | milliseconds | Lower is better | 
| Throughput | Overall Aggregates query rate | docs/sec | Higher is better | 
| Latency | Overall Aggregates query q50 latency | milliseconds | Lower is better | 
| Throughput | Overall Updates query rate | docs/sec | Higher is better | 
| Latency | Overall Updates query q50 latency | milliseconds | Lower is better | 