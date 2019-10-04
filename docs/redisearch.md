# FTSB Supplemental Guide: RediSearch

## Aggregation Queries Detail

Aggregations are a way to process the results of a search query, group, sort and transform them - and extract analytic insights from them. Much like aggregation queries in other databases and search engines, they can be used to create analytics reports, or perform Faceted Search style queries.

### Q1
#### One year period, Exact Number of contributions by day, ordered chronologically, for a given editor

```
FT.AGGREGATE $IDX "@CURRENT_REVISION_EDITOR_USERNAME: <username> @CURRENT_REVISION_TIMESTAMP:[<interval_start> <interval_end>]" \
             APPLY "@CURRENT_REVISION_TIMESTAMP - (@CURRENT_REVISION_TIMESTAMP % 86400)" AS day \
             GROUPBY 1 @day \
             REDUCE COUNT 1 @ID AS num_contributions \
             SORTBY 2 @day DESC MAX 365 \
             APPLY "timefmt(@day)" AS day
```
Real monitor example: 
```
FT.AGGREGATE" "pages-meta-idx1" "@CURRENT_REVISION_EDITOR_USERNAME:Sgeureka @CURRENT_REVISION_TIMESTAMP:[1345693011 1377229011]" "APPLY" "@CURRENT_REVISION_TIMESTAMP - (@CURRENT_REVISION_TIMESTAMP % 86400)" "AS" "day" "GROUPBY" "1" "@day" "REDUCE" "COUNT" "1" "@ID" "AS" "num_contributions" "SORTBY" "2" "@day" "DESC" "MAX" "365" "APPLY" "timefmt(@day)" "AS" "day"
```


### Q2
#### One month period, Exact Number of distinct editors contributions by hour, ordered chronologically

```
 FT.AGGREGATE $IDX "@CURRENT_REVISION_TIMESTAMP:[<interval_start> <interval_end>]" \
              APPLY "@CURRENT_REVISION_TIMESTAMP - (@CURRENT_REVISION_TIMESTAMP % 3600)" AS hour \
              GROUPBY 1 @hour \
                      REDUCE COUNT 1 @CURRENT_REVISION_EDITOR_USERNAME AS num_distinct_editors \
              SORTBY 2 @hour DESC MAX 720 \
              APPLY "timefmt(@hour)" AS hour
```
Real monitor example: 
```
"FT.AGGREGATE" "pages-meta-idx1" "@CURRENT_REVISION_TIMESTAMP:[1224484759 1227076759]" \
               "APPLY" "@CURRENT_REVISION_TIMESTAMP - (@CURRENT_REVISION_TIMESTAMP % 3600)" "AS" "hour" \
               "GROUPBY" "1" "@hour" \
                         "REDUCE" "COUNT" "1" "@CURRENT_REVISION_EDITOR_USERNAME" "AS" "num_distinct_editors" \
               "SORTBY" "2" "@hour" "DESC" "MAX" "720" \
               "APPLY" "timefmt(@hour)" "AS" "hour"
```
### Q3
#### One month period, Approximate Number of distinct editors contributions by hour, ordered chronologically

```
 FT.AGGREGATE $IDX "@CURRENT_REVISION_TIMESTAMP:[<interval_start> <interval_end>]" \
              APPLY "@CURRENT_REVISION_TIMESTAMP - (@CURRENT_REVISION_TIMESTAMP % 3600)" AS hour \
              GROUPBY 1 @hour \
                      REDUCE COUNT_DISTINCTISH 1 @CURRENT_REVISION_EDITOR_USERNAME AS num_distinct_editors \
              SORTBY 2 @hour DESC MAX 720 \
              APPLY "timefmt(@hour)" AS hour
```
Real monitor example: 
```
"FT.AGGREGATE" "pages-meta-idx1" "@CURRENT_REVISION_TIMESTAMP:[1224484759 1227076759]" \
               "APPLY" "@CURRENT_REVISION_TIMESTAMP - (@CURRENT_REVISION_TIMESTAMP % 3600)" "AS" "hour" \
               "GROUPBY" "1" "@hour" \
                         "REDUCE" "COUNT_DISTINCTISH" "1" "@CURRENT_REVISION_EDITOR_USERNAME" "AS" "num_distinct_editors" \
               "SORTBY" "2" "@hour" "DESC" "MAX" "720" \
               "APPLY" "timefmt(@hour)" "AS" "hour"
```

### Q4
#### One day period, Approximate Number of contributions by 5minutes interval by editor username, ordered first chronologically and second alphabetically by Revision editor username

```
FT.AGGREGATE $IDX "@CURRENT_REVISION_TIMESTAMP:[<interval_start> <interval_end>]" \
             APPLY "@CURRENT_REVISION_TIMESTAMP - (@CURRENT_REVISION_TIMESTAMP % 300)" AS fiveMinutes \
             GROUPBY 2 @fiveMinutes @CURRENT_REVISION_EDITOR_USERNAME \
                     REDUCE COUNT_DISTINCTISH 1 @ID AS num_contributions \
             FILTER '@CURRENT_REVISION_EDITOR_USERNAME != ""' \
             SORTBY 4 @fiveMinutes ASC @CURRENT_REVISION_EDITOR_USERNAME DESC MAX 288 \
             APPLY "timefmt(@fiveMinutes)" AS fiveMinutes
```
Real monitor example: 
```
"FT.AGGREGATE" "pages-meta-idx1" "@CURRENT_REVISION_TIMESTAMP:[1216967959 1217054359]" \
               "APPLY" "@CURRENT_REVISION_TIMESTAMP - (@CURRENT_REVISION_TIMESTAMP % 300)" "AS" "fiveMinutes" \
               "GROUPBY" "2" "@fiveMinutes" "@CURRENT_REVISION_EDITOR_USERNAME" \
                         "REDUCE" "COUNT_DISTINCTISH" "1" "@ID" "AS" "num_contributions" \
               "FILTER" "@CURRENT_REVISION_EDITOR_USERNAME !=\"\""\
               "SORTBY" "4" "@fiveMinutes" "ASC" "@CURRENT_REVISION_EDITOR_USERNAME" "DESC" "MAX" "288" \
                "APPLY" "timefmt(@fiveMinutes)" "AS" "fiveMinutes"
```

### Q5
#### One month period, Approximate Top 10 Revision editor usernames

```
FT.AGGREGATE $IDX "@CURRENT_REVISION_TIMESTAMP:[<interval_start> <interval_end>]" \
             GROUPBY 1 "@CURRENT_REVISION_EDITOR_USERNAME" \
                     REDUCE COUNT_DISTINCTISH 1 "@ID" AS num_contributions \
             FILTER '@CURRENT_REVISION_EDITOR_USERNAME != ""' \
             SORTBY 2 @num_contributions ASC MAX 10 \
             LIMIT 0 10
```
Real monitor example: 
```
"FT.AGGREGATE" "pages-meta-idx1" "@CURRENT_REVISION_TIMESTAMP:[1224484759 1227076759]" \
               "GROUPBY" "1" "@CURRENT_REVISION_EDITOR_USERNAME" \
                         "REDUCE" "COUNT_DISTINCTISH" "1" "@ID" "AS" "num_contributions" \
               "FILTER" "@CURRENT_REVISION_EDITOR_USERNAME !=\"\"" \
               "SORTBY" "2" "@num_contributions" "ASC" "MAX" "10" \
               "LIMIT" "0" "10"
```


### Q6
#### One month period, Approximate Top 10 Revision editor usernames by number of Revisions broken by namespace (TAG field).

```
FT.AGGREGATE $IDX "@CURRENT_REVISION_TIMESTAMP:[<interval_start> <interval_end>]" \
             GROUPBY 2 "@NAMESPACE" "@CURRENT_REVISION_EDITOR_USERNAME" \
                     REDUCE COUNT_DISTINCTISH 1 "@ID" AS num_contributions \
             FILTER '@CURRENT_REVISION_EDITOR_USERNAME != ""' \
             SORTBY 4 @NAMESPACE ASC @num_contributions ASC MAX 10 \
             LIMIT 0 10
```
Real monitor example: 
```
"FT.AGGREGATE" "pages-meta-idx1" "@CURRENT_REVISION_TIMESTAMP:[1224484759 1227076759]" \
               "GROUPBY" "2" "@NAMESPACE" "@CURRENT_REVISION_EDITOR_USERNAME" \
                         "REDUCE" "COUNT_DISTINCTISH" "1" "@ID" "AS" "num_contributions" \
               "FILTER" "@CURRENT_REVISION_EDITOR_USERNAME !=\"\"" \
               "SORTBY" "4" "@NAMESPACE" "ASC" "@num_contributions" "ASC" "MAX" "10" \
               "LIMIT" "0" "10"
```

### Q7
#### One month period, Top 10 editor username by average revision content.

```
FT.AGGREGATE $IDX "@CURRENT_REVISION_TIMESTAMP:[<interval_start> <interval_end>]" \
             GROUPBY 1 @CURRENT_REVISION_EDITOR_USERNAME \
                     REDUCE AVG 1 @CURRENT_REVISION_CONTENT_LENGTH AS avg_rcl \
             SORTBY 2 @avg_rcl DESC MAX 10 \
             LIMIT 0 10
```
Real monitor example: 
```
"FT.AGGREGATE" "pages-meta-idx1" "@CURRENT_REVISION_TIMESTAMP:[1224484759 1227076759]" \
               "GROUPBY" "2" "@NAMESPACE" "@CURRENT_REVISION_EDITOR_USERNAME" \
                         "REDUCE" "AVG" "1" "@CURRENT_REVISION_CONTENT_LENGTH" "AS" "avg_rcl" \
               "SORTBY" "2" "@avg_rcl" "DESC" "MAX" "10" \
               "LIMIT" "0" "10"
```


### Q8
#### One year period, Exact Number of contributions by day, ordered chronologically, for a given editor

```
FT.AGGREGATE $IDX "@CURRENT_REVISION_EDITOR_USERNAME: <username> @CURRENT_REVISION_TIMESTAMP:[<interval_start> <interval_end>]" \
             APPLY "year(@CURRENT_REVISION_TIMESTAMP)" AS year \
             GROUPBY 1 @year \
                     REDUCE COUNT 1 @ID AS num_contributions \
             SORTBY 2 @year ASC MAX 365 \
             APPLY "timefmt(@day)" 

```
Real monitor example: 
```
"FT.AGGREGATE" "pages-meta-idx1" "@CURRENT_REVISION_EDITOR_USERNAME:Sgeureka" \
               "APPLY" "@CURRENT_REVISION_TIMESTAMP - (@CURRENT_REVISION_TIMESTAMP % 86400)" "AS" "day" \
               "GROUPBY" "1" "@day" \
                         "REDUCE" "COUNT" "1" "@ID" "AS" "num_contributions" \
               "SORTBY" "2" "@day" "DESC" "MAX" "365" \
               "APPLY" "timefmt(@day)" "AS" "day"
 ```
