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
