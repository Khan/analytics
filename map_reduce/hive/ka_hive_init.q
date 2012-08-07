-- Initialization script for hive to recover tables mirroring GAE entities. Run
--
-- $ hive -i s3://ka-mapreduce/code/hive/ka_hive_init.q \
--        -d INPATH=s3://ka-mapreduce/entity_store
-- for your interactive hive shells.
--
-- Alternatively, you can source this file, but you need INPATH defined
-- prior to doing so.


--------------------------------------------------------------------------------
-- Datastore Entity Tables


CREATE EXTERNAL TABLE IF NOT EXISTS Exercise (
    key string, json string
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LOCATION '${INPATH}/Exercise';

CREATE EXTERNAL TABLE IF NOT EXISTS ExerciseVideo (
    key string, json string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '${INPATH}/ExerciseVideo';

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


--------------------------------------------------------------------------------
-- Summary Tables

-- Describes activity on a per-video basis for users on a given day
CREATE EXTERNAL TABLE IF NOT EXISTS user_video_summary(
  user STRING, video_key STRING, video_title STRING,
  num_seconds INT, completed BOOLEAN)
PARTITIONED BY (dt STRING)
LOCATION 's3://ka-mapreduce/summary_tables/user_video_summary';
ALTER TABLE user_video_summary RECOVER PARTITIONS;


-- Describes activity on a per-exercise basis for users on a given day
CREATE EXTERNAL TABLE IF NOT EXISTS user_exercise_summary(
  user STRING, exercise STRING, time_spent  INT,
  num_correct INT, num_wrong INT, proficient BOOLEAN) PARTITIONED BY (dt STRING)
LOCATION 's3://ka-mapreduce/summary_tables/user_exercise_summary';
ALTER TABLE user_exercise_summary RECOVER PARTITIONS;


-- Describes feedbacks on a per-video basis for users on a given day
-- Available since 6/1/2012
CREATE EXTERNAL TABLE IF NOT EXISTS user_feedback_summary(
  user STRING, video_key STRING, record_cnt INT,
  question_cnt INT, answer_cnt INT) PARTITIONED BY (dt STRING)
LOCATION 's3://ka-mapreduce/summary_tables/user_feedback_summary';
ALTER TABLE user_feedback_summary RECOVER PARTITIONS;


-- Consolidated view a user's activity on a given day.  
-- See user_daily_activity.q for details.
CREATE EXTERNAL TABLE IF NOT EXISTS user_daily_activity(
  user STRING,
  joined BOOLEAN,
  feedback_items INT,
  videos_started INT, videos_completed INT, videos_seconds INT,
  exercises_started INT, exercises_completed INT, 
  exercises_problems_done INT, exercises_seconds INT)
PARTITIONED BY (dt STRING)
LOCATION 's3://ka-mapreduce/summary_tables/user_daily_activity';
ALTER TABLE user_daily_activity RECOVER PARTITIONS;


-- Based on user_daily_activity, holds time series of total account status 
-- changes (e.g, activation, deactivation, ...) on daily, weekly, and 
-- monthly timescales.  
-- See user_growth.[q|py] for more details.
CREATE EXTERNAL TABLE IF NOT EXISTS user_growth(
  dt STRING,
  series STRING,
  value INT)
PARTITIONED BY (timescale STRING)
LOCATION 's3://ka-mapreduce/summary_tables/user_growth';
ALTER TABLE user_growth RECOVER PARTITIONS;


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

-- More user friendly topic mapping 
-- the keys and titles are sorted from generic to specific
CREATE EXTERNAL TABLE IF NOT EXISTS topic_mapping(
  topic_key STRING, topic_title STRING,
  ancestor_keys_json STRING, ancestor_titles_json STRING) 
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LOCATION 's3://ka-mapreduce/summary_tables/topic_mapping/';

CREATE EXTERNAL TABLE IF NOT EXISTS video_topic_category(
  vid_key STRING, top_category STRING, second_category STRING) 
  LOCATION 's3://ka-mapreduce/summary_tables/video_topic_category/';

-- TODO(benkomalo): when using ADD FILE with s3 paths, it downloads it to a
--    local cache, which we have to reference from directly. Figure out
--    a better solution for this, or specify the Hive version so that this
--    path is stable.
ADD FILE s3://ka-mapreduce/code/hive/create_topic_attempts.q;
SOURCE /mnt/var/lib/hive_081/downloaded_resources/create_topic_attempts.q;
ALTER TABLE topic_attempts RECOVER PARTITIONS;

CREATE EXTERNAL TABLE IF NOT EXISTS topic_retention_summary (
  topic STRING, user_segment STRING, is_randomized BOOLEAN, bucket_type STRING,
  bucket_value INT, num_correct INT, num_attempts INT
) COMMENT 'User retention on exercise topics over time or number of cards done'
PARTITIONED BY (start_dt STRING, end_dt STRING)
LOCATION 's3://ka-mapreduce/summary_tables/topic_retention_summary';
ALTER TABLE topic_retention_summary RECOVER PARTITIONS;

CREATE EXTERNAL TABLE IF NOT EXISTS accuracy_deltas_summary (
  topic STRING, user_segment STRING, num_problems_done INT, card_number INT,
  sum_deltas DOUBLE, num_deltas INT, avg_deltas DOUBLE)
COMMENT 'Average accuracy deltas across user segments and ending cards'
PARTITIONED BY (start_dt STRING, end_dt STRING)
LOCATION 's3://ka-mapreduce/summary_tables/accuracy_deltas_summary';
ALTER TABLE accuracy_deltas_summary RECOVER PARTITIONS;


--------------------------------------------------------------------------------
-- Utility files that custom Python mapper/reducer scripts can import.

-- NOTE: To import a utility file, you'll need to add the current directory to
-- Python's path before the import so Hadoop can pick it up:
--     sys.path.append(os.path.dirname(__file__))
--     import table_parser


ADD FILE s3://ka-mapreduce/code/py/table_parser.py;
