-- Updating bulk downloaded data
-- These are small factual tables we download daily. 
-- Each day's download is the whole snapshot of the table.
-- dt: datestamp for updating the bulkdownloaded data

CREATE EXTERNAL TABLE IF NOT EXISTS ExerciseP (
  key STRING, json STRING)
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '${INPATH}/ExerciseP/';
ALTER TABLE ExerciseP RECOVER PARTITIONS;

INSERT OVERWRITE TABLE Exercise
SELECT key, json FROM ExerciseP 
WHERE dt = '${dt}';


CREATE EXTERNAL TABLE IF NOT EXISTS ExerciseVideoP (
  key STRING, json STRING)
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '${INPATH}/ExerciseVideoP/';
ALTER TABLE ExerciseVideoP RECOVER PARTITIONS;

INSERT OVERWRITE TABLE ExerciseVideo
SELECT key, json FROM ExerciseVideoP 
WHERE dt = '${dt}';


CREATE EXTERNAL TABLE IF NOT EXISTS TopicP (
  key STRING, json STRING)
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '${INPATH}/TopicP/';
ALTER TABLE TopicP RECOVER PARTITIONS;

INSERT OVERWRITE TABLE Topic
SELECT key, json FROM TopicP 
WHERE dt = '${dt}';


CREATE EXTERNAL TABLE IF NOT EXISTS VideoP (
  key STRING, json STRING)
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '${INPATH}/VideoP/';
ALTER TABLE VideoP RECOVER PARTITIONS;

INSERT OVERWRITE TABLE Video
SELECT key, json FROM VideoP 
WHERE dt = '${dt}';

-- Populating topic_mapping table
-- Use DISTRIBUTE BY to make sure the code is run at reducing step
SET mapred.reduce.tasks=1;
FROM (
  SELECT key, json FROM Topic
  DISTRIBUTE BY key
) map_out 
INSERT OVERWRITE TABLE topic_mapping
SELECT TRANSFORM(map_out.key, map_out.json) 
USING 's3://ka-mapreduce/code/py/topic_mapping_reducer.py'
AS topic_key, topic_title, ancestor_keys_json, ancestor_titles_json;
SET mapred.reduce.tasks=-1;

INSERT OVERWRITE TABLE video_topic_category
SELECT DISTINCT a.vid_key, 
  get_json_object(b.ancestor_titles_json, '$.titles[1]'), 
  concat(get_json_object(b.ancestor_titles_json, '$.titles[1]'), '-', 
         get_json_object(b.ancestor_titles_json, '$.titles[2]')) 
FROM video_topic a LEFT OUTER JOIN topic_mapping b 
  ON (a.topic_key = b.topic_key);

-- Example to get video / category description
-- SELECT IF(second_category IS NULL, top_category, second_category), COUNT(1) 
-- FROM video_topic_category GROUP BY 
-- IF(second_category IS NULL, top_category, second_category);

