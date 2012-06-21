-- Initialization script for hive to recover tables mirroring GAE entities. Run
--
-- $ hive -i s3://ka-mapreduce/code/hive/ka_hive_init.q \
--        -d INPATH=s3://ka-mapreduce/entity_store
-- for your interactive hive shells.
--
-- Alternatively, you can source this file, but you need INPATH defined
-- prior to doing so.


--Datastore Entity Tables


CREATE EXTERNAL TABLE IF NOT EXISTS Exercise (
    key string, json string
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LOCATION '${INPATH}/Exercise';


CREATE EXTERNAL TABLE IF NOT EXISTS Feedback (
    key string, json string
  )
  PARTITIONED BY (dt string)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LOCATION '${INPATH}/Feedback';
ALTER TABLE Feedback RECOVER PARTITIONS;


CREATE EXTERNAL TABLE IF NOT EXISTS FeedbackVote (
    key string, json string
  )
  PARTITIONED BY (dt string)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LOCATION '${INPATH}/FeedbackVote';
ALTER TABLE FeedbackVote RECOVER PARTITIONS;


CREATE EXTERNAL TABLE IF NOT EXISTS ProblemLog (
    user string, json string
  )
  PARTITIONED BY (dt string)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LOCATION '${INPATH}/ProblemLog';
ALTER TABLE ProblemLog RECOVER PARTITIONS;


CREATE EXTERNAL TABLE IF NOT EXISTS StackLog (
    user string, json string
  )
  PARTITIONED BY (dt string)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LOCATION '${INPATH}/StackLog';
ALTER TABLE StackLog RECOVER PARTITIONS;


CREATE EXTERNAL TABLE IF NOT EXISTS Topic (
    key string, json string
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LOCATION '${INPATH}/Topic';


CREATE EXTERNAL TABLE IF NOT EXISTS UserBadge (
    user string, json string
  )
  PARTITIONED BY (dt string)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LOCATION '${INPATH}/UserBadge';
ALTER TABLE UserBadge RECOVER PARTITIONS;

--Getting the latest partition
ADD FILE s3://ka-mapreduce/conf/userdata_ver.q;
SOURCE /mnt/var/lib/hive_081/downloaded_resources/userdata_ver.q;

CREATE EXTERNAL TABLE IF NOT EXISTS UserDataP (
    key string, json string)
COMMENT 'UserData snapshots'
PARTITIONED BY (dt string) 
CLUSTERED BY (key) INTO 128 BUCKETS
LOCATION '${INPATH}/UserDataP';
ALTER TABLE UserDataP RECOVER PARTITIONS;

CREATE EXTERNAL TABLE IF NOT EXISTS UserDataIncr (
    key string, json string)
COMMENT 'Daily incremental user data updates'
PARTITIONED BY (dt string) 
CLUSTERED BY (key) INTO 16 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION 's3://ka-mapreduce/entity_store_incr/UserData';
ALTER TABLE UserDataIncr RECOVER PARTITIONS; 

DROP TABLE IF EXISTS UserData;
DROP VIEW IF EXISTS UserData;
CREATE VIEW UserData 
AS SELECT * FROM UserDataP 
WHERE dt = '${userdata_partition}'; 

CREATE EXTERNAL TABLE IF NOT EXISTS Video (
    key string, json string
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LOCATION '${INPATH}/Video';

CREATE EXTERNAL TABLE IF NOT EXISTS VideoLog (
    user string, json string
  )
  PARTITIONED BY (dt string)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LOCATION '${INPATH}/VideoLog';
ALTER TABLE VideoLog RECOVER PARTITIONS;


-- Summary Tables

-- Describes activity on a per-video basis for users on a given day
CREATE EXTERNAL TABLE IF NOT EXISTS user_video_summary(
  user STRING, video_key STRING, video_title STRING,
  num_seconds INT, completed INT)
PARTITIONED BY (dt STRING)
LOCATION 's3://ka-mapreduce/summary_tables/user_video_summary';
ALTER TABLE user_video_summary RECOVER PARTITIONS;


-- Defined in userdata_info.q
CREATE EXTERNAL TABLE IF NOT EXISTS userdata_info(
  user STRING,
  user_id STRING,
  user_email STRING,
  user_nickname STRING,
  joined DOUBLE,
  registered BOOLEAN
  )
LOCATION 's3://ka-mapreduce/summary_tables/userdata_info';

CREATE EXTERNAL TABLE IF NOT EXISTS video_topic(
  vid_key STRING, vid_title STRING, topic_key STRING,
  topic_title STRING, topic_desc STRING)
LOCATION 's3://ka-mapreduce/summary_tables/video_topic';

-- TODO(benkomalo): when using ADD FILE with s3 paths, it downloads it to a
--    local cache, which we have to reference from directly. Figure out
--    a better solution for this, or specify the Hive version so that this
--    path is stable.
ADD FILE s3://ka-mapreduce/code/hive/create_topic_attempts.q;
SOURCE /mnt/var/lib/hive_081/downloaded_resources/create_topic_attempts.q;
ALTER TABLE topic_attempts RECOVER PARTITIONS;
