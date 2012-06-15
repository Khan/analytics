-- Initialization script for hive to recover tables mirroring GAE entities. Run
-- "hive -d INPATH=s3://ka-mapreduce/entity_store -i s3://ka-mapreduce/code/hive/ka_hive_init.q"
-- for your interactive hive shells.


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


CREATE EXTERNAL TABLE IF NOT EXISTS UserData (
    key string, json string
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LOCATION '${INPATH}/UserData';


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


CREATE EXTERNAL TABLE IF NOT EXISTS user_video_summary(
  user STRING, video_key STRING, video_title STRING,
  num_seconds INT, completed INT)
PARTITIONED BY (dt STRING)
LOCATION 's3://ka-mapreduce/summary_tables/user_video_summary';
ALTER TABLE user_video_summary RECOVER PARTITIONS;


CREATE EXTERNAL TABLE IF NOT EXISTS userdata_ids(
  user STRING, user_id STRING, user_email STRING,
  current_user STRING, user_nickname STRING, joined DOUBLE)
LOCATION 's3://ka-mapreduce/summary_tables/userdata_ids';


CREATE EXTERNAL TABLE IF NOT EXISTS video_topic(
  vid_key STRING, vid_title STRING, topic_key STRING,
  topic_title STRING, topic_desc STRING)
LOCATION 's3://ka-mapreduce/summary_tables/video_topic';

ADD FILE s3://ka-mapreduce/code/hive/create_topic_attempts_summary_table.q;
SOURCE create_topic_attempts_summary_table.q;
ALTER TABLE topic_attempts_summary RECOVER PARTITIONS;

ADD FILE s3://ka-mapreduce/code/hive/create_topic_attempts.q;
SOURCE create_topic_attempts.q;
ALTER TABLE topic_attempts RECOVER PARTITIONS;
