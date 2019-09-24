#!/bin/bash

## Page metadata
#- Title of the page
#- Namespace of the page
#- Page id (for old versions these are shown in the URLs of page history links)

## Revision metadata
#- Id of the revision
#- Revision id of the previous (parent) revision
#- Date and time the edit was made
#- Username and user id, or IP address of the editor
#- Comment left by the editor when the edit was saved
#- Length in bytes of the revision content

IDX="pages-meta-idx1"
DOCPREFIX=$IDX"-doc"

redis-cli ft.drop $IDX SCHEMA

# create the index
redis-cli ft.create $IDX SCHEMA \
  TITLE TEXT SORTABLE\
  NAMESPACE TAG SORTABLE \
  ID NUMERIC SORTABLE \
  PARENT_REVISION_ID NUMERIC SORTABLE \
  CURRENT_REVISION_TIMESTAMP NUMERIC SORTABLE \
  CURRENT_REVISION_ID NUMERIC SORTABLE \
  CURRENT_REVISION_EDITOR_USERNAME TEXT NOSTEM SORTABLE \
  CURRENT_REVISION_EDITOR_IP TEXT \
  CURRENT_REVISION_EDITOR_USERID NUMERIC SORTABLE \
  CURRENT_REVISION_EDITOR_COMMENT TEXT \
  CURRENT_REVISION_CONTENT_LENGTH NUMERIC SORTABLE


#<page>
#    <title>Stockton Airport</title>
#    <ns>0</ns>
#    <id>7697612</id>
#    <revision>
#      <id>865514439</id>
#      <parentid>479135040</parentid>
#      <timestamp>2018-10-24T11:44:29Z</timestamp>
#      <contributor>
#        <username>Narky Blert</username>
#        <id>22041646</id>
#      </contributor>
#      <minor />
#      <comment>ce</comment>
#      <model>wikitext</model>
#      <sha1>qxcai6tfmnb22471c9xe3qamuejvst9</sha1>
#    </revision>
#  </page>

redis-cli FT.ADD $IDX $DOCPREFIX-7697612 1.0 FIELDS\
  TITLE "Stockton Airport"\
  NAMESPACE "0"\
  ID 7697612\
  PARENT_REVISION_ID 479135040\
  CURRENT_REVISION_ID 865514439\
  CURRENT_REVISION_TIMESTAMP 1540378169\
  CURRENT_REVISION_EDITOR_USERNAME "Narky Blert"\
  CURRENT_REVISION_EDITOR_USERID 22041646\
  CURRENT_REVISION_EDITOR_COMMENT "CE"\
  CURRENT_REVISION_CONTENT_LENGTH 2

#<page>
#  <title>Talk:Torque tube</title>
#  <ns>1</ns>
#  <id>7697694</id>
#  <revision>
#    <id>690321498</id>
#    <parentid>571774265</parentid>
#    <timestamp>2015-11-12T17:25:17Z</timestamp>
#    <contributor>
#      <username>CZmarlin</username>
#      <id>1606992</id>
#    </contributor>
#    <minor />
#    <comment>[[Wikipedia:WikiProject|WikiProject]] assessment</comment>
#    <model>wikitext</model>
#    <format>text/x-wiki</format>
#     <sha1>gdqpunkhd9y75vq3ilz6kp2huvx63zz</sha1>
#  </revision>
#</page>

redis-cli FT.ADD $IDX $DOCPREFIX-7697694 1.0 FIELDS\
  TITLE "Talk:Torque tube"\
  NAMESPACE "1"\
  ID 7697694\
  PARENT_REVISION_ID 571774265\
  CURRENT_REVISION_ID 690321498\
  CURRENT_REVISION_TIMESTAMP 1447349117\
  CURRENT_REVISION_EDITOR_USERNAME "CZmarlin"\
  CURRENT_REVISION_EDITOR_USERID 1606992\
  CURRENT_REVISION_EDITOR_COMMENT "[[Wikipedia:WikiProject|WikiProject]] assessment"\
  CURRENT_REVISION_CONTENT_LENGTH 50


###### more made up docs

redis-cli FT.ADD $IDX $DOCPREFIX-111 1.0 FIELDS\
  TITLE "Test same editor doc"\
  NAMESPACE "0"\
  ID 111\
  PARENT_REVISION_ID 571774265\
  CURRENT_REVISION_ID 1112\
  CURRENT_REVISION_TIMESTAMP 1427349117\
  CURRENT_REVISION_EDITOR_USERNAME "CZmarlin"\
  CURRENT_REVISION_EDITOR_USERID 1606992\
  CURRENT_REVISION_EDITOR_COMMENT "[[Wikipedia:WikiProject|WikiProject]] assessment"\
  CURRENT_REVISION_CONTENT_LENGTH 50


###### more made up docs

redis-cli FT.ADD $IDX $DOCPREFIX-1 1.0 FIELDS\
  TITLE "Test same editor doc"\
  NAMESPACE "0"\
  ID 1\
  PARENT_REVISION_ID 571774265\
  CURRENT_REVISION_ID 1\
  CURRENT_REVISION_TIMESTAMP 1427349110\
  CURRENT_REVISION_EDITOR_IP "1.1.1.1"\
  CURRENT_REVISION_EDITOR_USERNAME "CZmarlin"\
  CURRENT_REVISION_EDITOR_COMMENT "asdfghjkla"\
  CURRENT_REVISION_CONTENT_LENGTH 10

redis-cli FT.ADD $IDX $DOCPREFIX-2 1.0 FIELDS\
  TITLE "Test same editor doc"\
  NAMESPACE "0"\
  ID 2\
  PARENT_REVISION_ID 571774265\
  CURRENT_REVISION_ID 2\
  CURRENT_REVISION_TIMESTAMP 1427349130\
  CURRENT_REVISION_EDITOR_IP "1.1.1.1"\
  CURRENT_REVISION_EDITOR_USERNAME "Filipe"\
  CURRENT_REVISION_EDITOR_COMMENT "asdfghjklaasdfghjkla"\
  CURRENT_REVISION_CONTENT_LENGTH 20

redis-cli FT.ADD $IDX $DOCPREFIX-3 1.0 FIELDS\
  TITLE "Test same editor doc"\
  NAMESPACE "0"\
  ID 3\
  PARENT_REVISION_ID 571774265\
  CURRENT_REVISION_ID 2\
  CURRENT_REVISION_TIMESTAMP 1427349130\
  CURRENT_REVISION_EDITOR_IP "1.1.1.1"\
  CURRENT_REVISION_EDITOR_USERNAME "CZmarlin"\
  CURRENT_REVISION_EDITOR_COMMENT "[[Wikipedia:WikiProject|WikiProject]] abasdasdsda"\
  CURRENT_REVISION_CONTENT_LENGTH 49

# One year period, Exact Number of contributions by day, ordered chronologically
# agg-(apply-groupby1-reduce1-sortby1-apply)-1year-exact-page-contributions-by-day
echo ""
echo "---------------------------------------------------------------------------------"
echo "1) One year period, Exact Number of contributions by day, ordered chronologically"
echo "---------------------------------------------------------------------------------"
redis-cli FT.AGGREGATE $IDX "*" \
  APPLY "@CURRENT_REVISION_TIMESTAMP - (@CURRENT_REVISION_TIMESTAMP % 86400)" AS day \
  GROUPBY 1 @day \
  REDUCE COUNT 1 @ID AS num_contributions \
  SORTBY 2 @day DESC MAX 365  \
  APPLY "timefmt(@day)" AS day


# 2) One month period, Exact Number of distinct editors contributions by hour, ordered chronologically
# agg-(apply-groupby1-reduce1-sortby1-apply)-1month-exact-distinct-editors-by-hour
echo ""
echo "---------------------------------------------------------------------------------"
echo "2) One month period, Exact Number of distinct editors contributions by hour, ordered chronologically"
echo "---------------------------------------------------------------------------------"

redis-cli FT.AGGREGATE $IDX "*" \
  APPLY "@CURRENT_REVISION_TIMESTAMP - (@CURRENT_REVISION_TIMESTAMP % 3600)" AS hour \
  GROUPBY 1 @hour \
  REDUCE COUNT 1 @CURRENT_REVISION_EDITOR_USERNAME AS num_distinct_editors \
  SORTBY 2 @hour DESC MAX 720  \
  APPLY "timefmt(@hour)" AS hour


# 3) One month period, Approximate Number of distinct editors contributions by hour, ordered chronologically
# agg-(apply-groupby1-approx.reduce1-sortby1-apply)-1month-approximate-distinct-editors-by-hour
echo ""
echo "---------------------------------------------------------------------------------"
echo "3) One month period, Approximate Number of distinct editors contributions by hour, ordered chronologically"
echo "---------------------------------------------------------------------------------"

redis-cli FT.AGGREGATE $IDX "*" \
  APPLY "@CURRENT_REVISION_TIMESTAMP - (@CURRENT_REVISION_TIMESTAMP % 3600)" AS hour \
  GROUPBY 1 @hour \
  REDUCE COUNT_DISTINCTISH 1 @CURRENT_REVISION_EDITOR_USERNAME AS num_distinct_editors \
  SORTBY 2 @hour DESC MAX 720 \
  APPLY "timefmt(@hour)" AS hour


# 4) One day period, Approximate Number of contributions by 5minutes interval by editor username, ordered first chronologically and second alphabetically by Revision editor username
# agg-(apply-groupby2-approx.reduce1-filter1-sortby2-apply)-1day-approximate-page-contributions-by-5minutes-by-editor-username
echo ""
echo "---------------------------------------------------------------------------------"
echo "4) One day period, Approximate Number of contributions by 5minutes interval by editor username, ordered first chronologically and second alphabetically by Revision editor username"
echo "---------------------------------------------------------------------------------"

redis-cli FT.AGGREGATE $IDX "*" \
  APPLY "@CURRENT_REVISION_TIMESTAMP - (@CURRENT_REVISION_TIMESTAMP % 300)" AS fiveMinutes \
  GROUPBY 2 @fiveMinutes @CURRENT_REVISION_EDITOR_USERNAME \
  REDUCE COUNT_DISTINCTISH 1 @ID AS num_contributions \
  FILTER '@CURRENT_REVISION_EDITOR_USERNAME != ""'\
  SORTBY 4 @fiveMinutes ASC @CURRENT_REVISION_EDITOR_USERNAME DESC MAX 288 \
  APPLY "timefmt(@fiveMinutes)" AS fiveMinutes


# 5) Aproximate All time Top 10 Revision editor usernames
# agg-(groupby1-aprox.reduce1-filter1-sortby1-limit1)-aproximate-top10-editor-usernames
echo ""
echo "---------------------------------------------------------------------------------"
echo "5) Aproximate All time Top 10 Revision editor usernames of all namespaces"
echo "---------------------------------------------------------------------------------"

redis-cli FT.AGGREGATE $IDX "*" \
  GROUPBY 1 "@CURRENT_REVISION_EDITOR_USERNAME" \
  REDUCE COUNT_DISTINCTISH 1 "@ID" AS num_contributions \
  FILTER '@CURRENT_REVISION_EDITOR_USERNAME != ""'\
  SORTBY 2 @num_contributions ASC \
  LIMIT 0 10

# 6) Aproximate All time Top 10 Revision editor usernames by namespace (TAG field)
# agg-(groupby2-aprox.reduce1-filter1-sortby2-limit1)-aproximate-top10-editor-usernames-by-namespace
echo ""
echo "---------------------------------------------------------------------------------"
echo "6) Aproximate All time Top 10 Revision editor usernames by namespace (TAG field)"
echo "---------------------------------------------------------------------------------"

redis-cli FT.AGGREGATE $IDX "*" \
  GROUPBY 2 "@NAMESPACE" "@CURRENT_REVISION_EDITOR_USERNAME"\
  REDUCE COUNT_DISTINCTISH 1 "@ID" AS num_contributions \
  FILTER '@CURRENT_REVISION_EDITOR_USERNAME != ""'\
  SORTBY 4 @NAMESPACE ASC @num_contributions ASC \
  LIMIT 0 10


# 7) Top 10 editor username by average revision content
# agg-(groupby1-reduce-sortby1-limit1)-avg-revision-content-length-by-editor-username
echo ""
echo "---------------------------------------------------------------------------------"
echo "7) Top 10 editor username by average revision content"
echo "---------------------------------------------------------------------------------"

redis-cli FT.AGGREGATE $IDX "*" \
  GROUPBY 1 @CURRENT_REVISION_EDITOR_USERNAME \
  REDUCE AVG 1 @CURRENT_REVISION_CONTENT_LENGTH AS avg_rcl \
  SORTBY 2 @avg_rcl DESC \
  LIMIT 0 10

# 8) Aproximate average number of contributions by year each editor makes
# agg-(apply-groupby1-reduce2-apply-sortby1)-aproximate-avg-editor-contributions-by-year
echo ""
echo "---------------------------------------------------------------------------------"
echo "8) Aproximate average number of contributions by year each editor makes"
echo "---------------------------------------------------------------------------------"

redis-cli FT.AGGREGATE $IDX "*" \
  APPLY "year(@CURRENT_REVISION_TIMESTAMP)" AS year \
  GROUPBY 1 @year \
  REDUCE COUNT 1 @ID AS num_contributions \
  REDUCE COUNT_DISTINCTISH 1 @CURRENT_REVISION_EDITOR_USERNAME AS num_distinct_editors \
  APPLY "@num_contributions / @num_distinct_editors" AS avg_num_contributions_by_editor \
  SORTBY 2 @year ASC

