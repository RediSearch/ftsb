
## Current use cases

Currently, FTSB supports three use cases:
 - **ecommerce-inventory**, From a base dataset of [10K fashion products on Amazon.com](https://data.world/promptcloud/fashion-products-on-amazon-com/workspace/file?filename=amazon_co-ecommerce_sample.csv) which are then multiplexed by categories, sellers, and countries to produce larger datasets > 1M docs. This benchmark focuses on updates and aggregate performance, splitting into Reads (FT.AGGREGATE), Cursor Reads (FT.CURSOR), and Updates (FT.ADD) the performance numbers. 
 The use case generates an index with 10 TAG fields (3 sortable and 1 non indexed), and 16 NUMERIC sortable non indexed fields per document.
 The aggregate queries are designed to be extremely costly both on computation and network TX, given that on each query we're aggregating and filtering over a large portion of the dataset while additionally loading 21 fields. 
 Both the update and read rates can be adjusted.
 
 
 - **enwiki-abstract**, From English-language [Wikipedia:Database](https://en.wikipedia.org/wiki/Wikipedia:Database_download) page abstracts. This use case generates
3 TEXT fields per document.


 - **enwiki-pages**, From English-language [Wikipedia:Database](https://en.wikipedia.org/wiki/Wikipedia:Database_download) last page revisions, containing processed metadata  extracted from the full Wikipedia XML dump.
 This use case generates 4 TEXT fields ( 2 sortable ), 1 sortable TAG field, and 6 sortable NUMERIC fields per document.
              
              

## Appendix I: Query types <a name="appendix-i-query-types"></a>

### Appendix I.I - English-language [Wikipedia:Database](https://en.wikipedia.org/wiki/Wikipedia:Database_download) page abstracts.
#### Full text search queries
|Query type|Description|Example|Status|
|:---|:---|:---|:---|
|simple-1word-query| Simple 1 Word Query | `Abraham` | :heavy_check_mark:
|2word-union-query| 2 Word Union Query | `Abraham Lincoln` | :heavy_check_mark:
|2word-intersection-query| 2 Word Intersection Query| `Abraham`&#124;`Lincoln` | :heavy_check_mark:
|exact-3word-match| Exact 3 Word Match| `"President Abraham Lincoln"` |:heavy_multiplication_x:
|autocomplete-1100-top3| Autocomplete -1100 Top 2-3 Letter Prefixes|  | :heavy_multiplication_x:
|2field-2word-intersection-query| 2 Fields, one word each, Intersection query | `@text_field1: text_value1 @text_field2: text_value2` | :heavy_multiplication_x:
|2field-1word-intersection-1numeric-range-query| 2 Fields, one text and another numeric, Intersection and numeric range query | `@text_field: text_value @numeric_field:[{min} {max}]` |:heavy_multiplication_x:

#### Spell Check queries

Performs spelling correction on a query, returning suggestions for misspelled terms.
To simmulate misspelled terms, for each word a deterministic random number of edits in the range 0..Min(word.length/2 , 4) is chosen. 


For each edit a random type of edit (delete, insert random char, replace with random char, switch adjacent chars).

|Query type|Description|Example|Status|
|:---|:---|:---|:---|
| simple-1word-spellcheck | Simple 1 Word Spell Check Query | `FT.SPELLCHECK {index} reids DISTANCE 1` | :heavy_check_mark:

#### Autocomplete queries
|Query type|Description|Example|Status|
|:---|:---|:---|:---|
| |  | `` | :heavy_multiplication_x:


#### Aggregate queries

Aggregations are a way to process the results of a search query, group, sort and transform them - and extract analytic insights from them. Much like aggregation queries in other databases and search engines, they can be used to create analytics reports, or perform Faceted Search style queries. 

|Query type|Description|Clauses included|Status|
|:---|:---|:---|:---|
| |  | `` | :heavy_multiplication_x:

#### Synonym queries
|Query type|Description|Example|Status|
|:---|:---|:---|:---|
| |  | `` | :heavy_multiplication_x:


### Appendix I.II - English-language [Wikipedia:Database](https://en.wikipedia.org/wiki/Wikipedia:Database_download) last page revisions.

#### Aggregate queries

Aggregations are a way to process the results of a search query, group, sort and transform them - and extract analytic insights from them. Much like aggregation queries in other databases and search engines, they can be used to create analytics reports, or perform Faceted Search style queries. 

|Query #|Query type|Description| Status|
|:---|:---|:---|:---|
| 1 | agg-1-editor-1year-exact-page-contributions-by-day |  One year period, Exact Number of contributions by day, ordered chronologically, for a given editor [(supplemental docs)](docs/redisearch.md#Q1) | :heavy_check_mark:
| 2 | agg-2-*-1month-exact-distinct-editors-by-hour | One month period, Exact Number of distinct editors contributions by hour, ordered chronologically  [(supplemental docs)](docs/redisearch.md#Q2) |:heavy_check_mark:
| 3 | agg-3-*-1month-approximate-distinct-editors-by-hour | One month period, Approximate Number of distinct editors contributions by hour, ordered chronologically  [(supplemental docs)](docs/redisearch.md#Q3) | :heavy_check_mark:
| 4 | agg-4-*-1day-approximate-page-contributions-by-5minutes-by-editor-username | One day period, Approximate Number of contributions by 5minutes interval by editor username, ordered first chronologically and second alphabetically by Revision editor username  [(supplemental docs)](docs/redisearch.md#Q4) |:heavy_check_mark:
| 5 | agg-5-*-1month-approximate-top10-editor-usernames | One month period, Approximate All time Top 10 Revision editor usernames. [(supplemental docs)](docs/redisearch.md#Q5) | :heavy_check_mark:
| 6 | agg-6-*-1month-approximate-top10-editor-usernames-by-namespace |  One month period, Approximate All time Top 10 Revision editor usernames by number of Revisions broken by namespace (TAG field) [(supplemental docs)](docs/redisearch.md#Q6) | :heavy_check_mark:
| 7 | agg-7-*-1month-avg-revision-content-length-by-editor-username |  One month period, Top 10 editor username by average revision content [(supplemental docs)](docs/redisearch.md#Q7) | :heavy_check_mark:
| 8 | agg-8-editor-approximate-avg-editor-contributions-by-year |  Approximate average number of contributions by year each editor makes [(supplemental docs)](docs/redisearch.md#Q8) | :heavy_check_mark:



    