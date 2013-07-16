-- Initialization script for hive to recover tables mirroring GAE entities. Run
--
-- $ hive -i s3://ka-mapreduce/code/hive/ka_hive_init.q \
--        -d INPATH=s3://ka-mapreduce/entity_store
-- for your interactive hive shells.
--
-- Note that userdata_partition specifies the latest snapshot date for all
-- entities that require daily snapshotting (i.e. entities that can mutate),
-- not just UserData.
--
-- Alternatively, you can source this file, but you need INPATH and
-- userdata_partition defined prior to doing so.


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

CREATE EXTERNAL TABLE IF NOT EXISTS ScratchpadRevision (
    key STRING, json STRING
  )
  PARTITIONED BY (dt STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LOCATION '${INPATH}/ScratchpadRevision';
ALTER TABLE ScratchpadRevision RECOVER PARTITIONS;

--------------------------------------------------------------------------------
-- Incrementally fetched entities

-- UserData entities (and _GAEBingoIdentityRecord, etc, up to "Summary Tables"
-- below) entities are downloaded daily in incremental updates,
-- and collected periodically into snapshots. The latest
-- snapshot version is defined in the following file:
-- (see userdata_update.q for generation of these partitions)
ADD FILE s3://ka-mapreduce/conf/userdata_ver.q;
SOURCE /mnt/var/lib/hive_081/downloaded_resources/userdata_ver.q;

-- A snapshot of updated UserData info, built by collecting UserDataIncr data
-- across multiple days and creating a collective snapshot.
-- The partition date dictates up to which date that snapshot has
-- been computed for (inclusive).
CREATE EXTERNAL TABLE IF NOT EXISTS UserDataP (
    key string, json string)
COMMENT 'UserData snapshots (created from multiple UserDataIncr partitions)'
PARTITIONED BY (dt string)
CLUSTERED BY (key) INTO 128 BUCKETS
LOCATION '${INPATH}/UserDataP';
ALTER TABLE UserDataP RECOVER PARTITIONS;

-- This stores daily subsets of UserData entities (ones that have been
-- modified on the partition date, and therefore needs updating).
CREATE EXTERNAL TABLE IF NOT EXISTS UserDataIncr (
    key string, json string)
COMMENT 'Daily incremental UserData updates'
PARTITIONED BY (dt string)
CLUSTERED BY (key) INTO 16 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION 's3://ka-mapreduce/entity_store_incr/UserData';
ALTER TABLE UserDataIncr RECOVER PARTITIONS;

-- A view of the latest UserDataP snapshot.
DROP TABLE IF EXISTS UserData;
DROP VIEW IF EXISTS UserData;
CREATE VIEW UserData
AS SELECT * FROM UserDataP
WHERE dt = '${userdata_partition}';

-- Samples for easy testing
DROP VIEW IF EXISTS UserDataSample;
CREATE VIEW UserDataSample
AS SELECT * FROM UserDataP
TABLESAMPLE(BUCKET 1 OUT OF 128 ON key)
WHERE dt = '${userdata_partition}';

-- Same things as UserData has above, but for GAEBingoIdentityRecord
CREATE EXTERNAL TABLE IF NOT EXISTS GAEBingoIdentityRecordP (
    key string,  -- Bingo identity string
    json string  -- JSONified form of BingoIdentityCache
  )
COMMENT 'GAEBingoIdentityRecord snapshots (created from multiple GAEBingoIdentityRecordIncr partitions)'
PARTITIONED BY (dt string)
CLUSTERED BY (key) INTO 128 BUCKETS
LOCATION '${INPATH}/GAEBingoIdentityRecordP';
ALTER TABLE GAEBingoIdentityRecordP RECOVER PARTITIONS;

CREATE EXTERNAL TABLE IF NOT EXISTS GAEBingoIdentityRecordIncr (
    key string,  -- Bingo identity string
    json string  -- JSONified form of BingoIdentityCache
  )
COMMENT 'Daily incremental GAEBingoIdentityRecord updates'
PARTITIONED BY (dt string)
CLUSTERED BY (key) INTO 16 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION 's3://ka-mapreduce/entity_store_incr/GAEBingoIdentityRecord';
ALTER TABLE GAEBingoIdentityRecordIncr RECOVER PARTITIONS;

DROP TABLE IF EXISTS GAEBingoIdentityRecord;
DROP VIEW IF EXISTS GAEBingoIdentityRecord;
CREATE VIEW GAEBingoIdentityRecord
AS SELECT * FROM GAEBingoIdentityRecordP
WHERE dt = '${userdata_partition}';

DROP VIEW IF EXISTS GAEBingoIdentityRecordSample;
CREATE VIEW GAEBingoIdentityRecordSample
AS SELECT * FROM GAEBingoIdentityRecordP
TABLESAMPLE(BUCKET 1 OUT OF 128 ON key)
WHERE dt = '${userdata_partition}';

-- This stores daily subsets of Feedback entities (ones that have been
-- modified on the partition date, and therefore needs updating).
CREATE EXTERNAL TABLE IF NOT EXISTS FeedbackIncr (
    key string, json string)
COMMENT 'Daily incremental Feedback updates'
PARTITIONED BY (dt string)
CLUSTERED BY (key) INTO 16 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION 's3://ka-mapreduce/entity_store_incr/Feedback';
ALTER TABLE FeedbackIncr RECOVER PARTITIONS;

CREATE EXTERNAL TABLE IF NOT EXISTS FeedbackP (
    key string,  -- GAE entity key for the Feedback
    json string)
COMMENT 'Feedback snapshots (created from multiple FeedbackIncr partitions)'
PARTITIONED BY (dt string)
CLUSTERED BY (key) INTO 32 BUCKETS
LOCATION '${INPATH}/FeedbackP';
ALTER TABLE FeedbackP RECOVER PARTITIONS;

DROP TABLE IF EXISTS Feedback;
DROP VIEW IF EXISTS Feedback;
CREATE VIEW Feedback
AS SELECT * FROM FeedbackP
WHERE dt = '${userdata_partition}';

DROP VIEW IF EXISTS FeedbackSample;
CREATE VIEW FeedbackSample
AS SELECT * FROM FeedbackP
TABLESAMPLE(BUCKET 1 OUT OF 16 ON key)
WHERE dt = '${userdata_partition}';

-- Scratchpads...
CREATE EXTERNAL TABLE IF NOT EXISTS ScratchpadIncr (key string, json string)
COMMENT 'Daily incremental Scratchpad updates'
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION 's3://ka-mapreduce/entity_store_incr/Scratchpad';
ALTER TABLE ScratchpadIncr RECOVER PARTITIONS;

CREATE EXTERNAL TABLE IF NOT EXISTS ScratchpadP (key string, json string)
COMMENT 'Scratchpad snapshots (created from multiple ScratchpadIncr partitions)'
PARTITIONED BY (dt string)
LOCATION '${INPATH}/ScratchpadP';
ALTER TABLE ScratchpadP RECOVER PARTITIONS;

DROP TABLE IF EXISTS Scratchpad;
DROP VIEW IF EXISTS Scratchpad;
CREATE VIEW Scratchpad
AS SELECT * FROM ScratchpadP
WHERE dt = '${userdata_partition}';

-- UserAssessment...
CREATE EXTERNAL TABLE IF NOT EXISTS UserAssessmentIncr (key string, json string)
COMMENT 'Daily incremental UserAssessment updates'
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION 's3://ka-mapreduce/entity_store_incr/UserAssessment';
ALTER TABLE UserAssessmentIncr RECOVER PARTITIONS;

CREATE EXTERNAL TABLE IF NOT EXISTS UserAssessmentP (key string, json string)
COMMENT 'UserAssessment snapshots (created from multiple UserAssessmentIncr partitions)'
PARTITIONED BY (dt string)
LOCATION '${INPATH}/UserAssessmentP';
ALTER TABLE UserAssessmentP RECOVER PARTITIONS;

DROP TABLE IF EXISTS UserAssessment;
DROP VIEW IF EXISTS UserAssessment;
CREATE VIEW UserAssessment
AS SELECT * FROM UserAssessmentP
WHERE dt = '${userdata_partition}';


--------------------------------------------------------------------------------
-- Summary Tables

-- Mirror of _GAEBingoAlternative and _GAEBingoExperiment entities on
-- prod (denormalized so that experiment info is in each alternative).
DROP TABLE IF EXISTS bingo_alternative_infoP;
CREATE EXTERNAL TABLE IF NOT EXISTS bingo_alternative_infoP(
    canonical_name string,  -- Canonical name of the experiment
    name string,  -- Name of the alternative
    hashable_name string,  -- Family or canonical name
    weight INT,
    dt_start string,
    live BOOLEAN,
    number INT)
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '${INPATH}/bingo_alternative_infoP';
ALTER TABLE bingo_alternative_infoP RECOVER PARTITIONS;

-- The entire set is snapshotted each day, and a VIEW is created for the
-- latest one (synced with UserData partition snapshots).
DROP TABLE IF EXISTS bingo_alternative_info;
DROP VIEW IF EXISTS bingo_alternative_info;
CREATE VIEW bingo_alternative_info
AS SELECT * FROM bingo_alternative_infoP
WHERE dt = '${userdata_partition}';

-- Describes activity on a per-video basis for users on a given day
CREATE EXTERNAL TABLE IF NOT EXISTS user_video_summary(
  user STRING, video_key STRING, video_title STRING,
  num_seconds BIGINT, completed BOOLEAN)
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


-- Stores monthly time series of registrations, long-term users
-- and engagement.  Populated in company_metrics.q
DROP TABLE IF EXISTS company_metrics;
CREATE EXTERNAL TABLE IF NOT EXISTS company_metrics(
  registrations_this_month INT,
  long_term_users_active_this_month INT,
  highly_engaged_users_active_this_month INT)
PARTITIONED BY (activity_month STRING)
LOCATION 's3://ka-mapreduce/summary_tables/company_metrics';
ALTER TABLE company_metrics RECOVER PARTITIONS;


-- Defined in userdata_update.q
-- is_coached: if user is coached
-- is_student: if user is coached by a coach who coached
--             >= 10 people
DROP TABLE IF EXISTS userdata_info_p;
CREATE EXTERNAL TABLE IF NOT EXISTS userdata_info_p(
  user STRING,
  user_id STRING,
  user_email STRING,
  user_nickname STRING,
  bingo_id STRING,
  joined DOUBLE,
  registered BOOLEAN,
  is_coached BOOLEAN,
  is_student BOOLEAN
  )
PARTITIONED BY (dt STRING)
LOCATION 's3://ka-mapreduce/summary_tables/userdata_info_p';
ALTER TABLE userdata_info_p RECOVER PARTITIONS;

DROP TABLE IF EXISTS userdata_info;
DROP VIEW IF EXISTS userdata_info;
CREATE VIEW userdata_info
AS SELECT * FROM userdata_info_p
WHERE dt = '${userdata_partition}';

-- Coach summary
CREATE EXTERNAL TABLE IF NOT EXISTS coach_summary (
  coach STRING,
  num_students INT,
  self_coaching BOOLEAN
) LOCATION 's3://ka-mapreduce/summary_tables/coach_summary';

CREATE EXTERNAL TABLE IF NOT EXISTS user_coach_summary(
  user STRING,
  num_coaches INT,
  max_coach_students INT
) LOCATION 's3://ka-mapreduce/summary_tables/user_coach_summary';

-- Number of students and teachers on a given day
DROP TABLE IF EXISTS student_teacher_count;
CREATE EXTERNAL TABLE IF NOT EXISTS student_teacher_count (
    teacher_count INT,
    student_count INT,
    coach_count INT,
    user_coach_count INT,
    active_teacher_count INT,
    active_student_count INT,
    active_coach_count INT,
    active_user_coach_count INT,
    teacher_visits_count INT,
    coach_visits_count INT,
    dt STRING
) LOCATION 's3://ka-mapreduce/summary_tables/student_teacher_count';

DROP TABLE IF EXISTS class_size_histogram;
CREATE EXTERNAL TABLE IF NOT EXISTS class_size_histogram (
    student_count INT,
    class_count INT
) LOCATION 's3://ka-mapreduce/summary_tables/class_size_histogram';

DROP TABLE IF EXISTS daily_class_profile_visits;
CREATE EXTERNAL TABLE IF NOT EXISTS daily_class_profile_visits (
    bingo_id STRING,
    url STRING
) PARTITIONED BY (dt STRING)
LOCATION 's3://ka-mapreduce/summary_tables/daily_class_profile_visits';
ALTER TABLE daily_class_profile_visits RECOVER PARTITIONS;

-- Holds geolocated summary of teachers
DROP TABLE IF EXISTS teacher_country;
CREATE EXTERNAL TABLE IF NOT EXISTS teacher_country (
    teacher STRING,
    student_count INT,
    user_id STRING,
    user_email STRING,
    user_nickname STRING,
    joined DOUBLE,
    ip STRING,
    city STRING,
    region STRING,
    country_code STRING,
    country STRING,
    latitude FLOAT,
    longitude FLOAT
) LOCATION 's3://ka-mapreduce/summary_tables/teacher_country';

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

-- Website request logs
DROP TABLE IF EXISTS website_request_logs;
CREATE EXTERNAL TABLE IF NOT EXISTS website_request_logs (
    ip STRING, user STRING, time_stamp STRING, method STRING, url STRING,
    protocol STRING, status INT, bytes INT, referer STRING,
    ms INT, cpu_ms INT, cpm_usd DOUBLE, queue_name STRING, pending_ms INT,
    url_route STRING, bingo_id STRING, kalog STRING
  )
  PARTITIONED BY (dt STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LOCATION 's3://ka-mapreduce/rawdata_tables/request_logs/website/';
ALTER TABLE website_request_logs RECOVER PARTITIONS;

DROP TABLE IF EXISTS daily_request_log_url_stats;
CREATE EXTERNAL TABLE IF NOT EXISTS daily_request_log_url_stats (
    count INT, url STRING, avg_response_bytes INT,
    ms_pct5 INT, ms_pct50 INT, ms_pct95 INT,
    cpu_ms_pct5 INT, cpu_ms_pct50 INT, cpu_ms_pct95 INT,
    cpm_microcents_pct5 INT, cpm_microcents_pct50 INT, cpm_microcents_pct95 INT,
    ms_pct25 INT, ms_pct75 INT,
    cpu_ms_pct25 INT, cpu_ms_pct75 INT,
    cpm_microcents_pct25 INT, cpm_microcents_pct75 INT
  )
  PARTITIONED BY (dt STRING)
  LOCATION 's3://ka-mapreduce/summary_tables/daily_request_log_url_stats';
ALTER TABLE daily_request_log_url_stats RECOVER PARTITIONS;

DROP TABLE IF EXISTS daily_request_log_urlroute_stats;
CREATE EXTERNAL TABLE IF NOT EXISTS daily_request_log_urlroute_stats (
    count INT, url_route STRING, avg_response_bytes INT,
    ms_pct5 INT, ms_pct50 INT, ms_pct95 INT,
    cpu_ms_pct5 INT, cpu_ms_pct50 INT, cpu_ms_pct95 INT,
    cpm_microcents_pct5 INT, cpm_microcents_pct50 INT, cpm_microcents_pct95 INT,
    ms_pct25 INT, ms_pct75 INT,
    cpu_ms_pct25 INT, cpu_ms_pct75 INT,
    cpm_microcents_pct25 INT, cpm_microcents_pct75 INT
  )
  PARTITIONED BY (dt STRING)
  LOCATION 's3://ka-mapreduce/summary_tables/daily_request_log_urlroute_stats';
ALTER TABLE daily_request_log_urlroute_stats RECOVER PARTITIONS;

DROP TABLE IF EXISTS exercise_summary;
CREATE EXTERNAL TABLE IF NOT EXISTS exercise_summary (
    exercise STRING,
    sub_exercise_type STRING,
    correct_attempts INT,
    wrong_attempts INT,
    time_taken INT,
    is_perseus BOOLEAN
  )
  PARTITIONED BY (dt STRING)
  LOCATION 's3://ka-mapreduce/summary_tables/exercise_summary';
ALTER TABLE exercise_summary RECOVER PARTITIONS;

-- Summary of users badges per context
DROP TABLE IF EXISTS badge_context_summary;
CREATE EXTERNAL TABLE IF NOT EXISTS badge_context_summary (
  badge_name STRING,
  context_name STRING,
  total_awarded INT,
  unique_awarded INT,
  total_points_earned INT
  )
  PARTITIONED BY (dt STRING)
  LOCATION 's3://ka-mapreduce/summary_tables/badge_context_summary';
ALTER TABLE badge_context_summary RECOVER PARTITIONS;


-- Summary of users badges
DROP TABLE IF EXISTS badge_summary;
CREATE EXTERNAL TABLE IF NOT EXISTS badge_summary (
  badge_name STRING,
  total_awarded INT,
  unique_awarded INT,
  total_points_earned INT
  )
  PARTITIONED BY (dt STRING)
  LOCATION 's3://ka-mapreduce/summary_tables/badge_summary';
ALTER TABLE badge_summary RECOVER PARTITIONS;


DROP TABLE IF EXISTS topic_old_key_name;
CREATE EXTERNAL TABLE IF NOT EXISTS topic_old_key_name (
    slug STRING,
    title STRING,
    standalone_title STRING,
    old_key_name STRING
) LOCATION 's3://ka-mapreduce/summary_tables/topic_old_key_name';


DROP TABLE IF EXISTS exercise_proficiency_summary;
CREATE EXTERNAL TABLE IF NOT EXISTS exercise_proficiency_summary (
    exercise STRING,
    earned_proficiency INT,
    total_users INT
  )
  LOCATION 's3://ka-mapreduce/summary_tables/exercise_proficiency_summary';


-- TODO(yunfang): deprecate the following table and move to video_stats
CREATE EXTERNAL TABLE IF NOT EXISTS daily_video_stats (
  title STRING,
  youtube_id STRING,
  watched INT,
  completed INT,
  seconds_watched BIGINT
) PARTITIONED BY (dt STRING, user_category STRING, aggregation STRING)
LOCATION 's3://ka-mapreduce/summary_tables/daily_video_stats';
ALTER TABLE daily_video_stats RECOVER PARTITIONS;


-- Partition information
-- duration: month|week|day. aka time_scale
-- dt: beginning date of the period
-- user_category: all|registered|phatom
-- aggregation: video|topic|total
CREATE EXTERNAL TABLE IF NOT EXISTS video_stats (
  title STRING,
  youtube_id STRING,
  users INT,
  visits INT,
  completed INT,
  seconds_watched BIGINT
) PARTITIONED BY (duration STRING, dt STRING,
                  user_category STRING, aggregation STRING)
LOCATION 's3://ka-mapreduce/summary_tables/video_stats';
ALTER TABLE video_stats RECOVER PARTITIONS;


CREATE EXTERNAL TABLE IF NOT EXISTS daily_exercise_stats (
    super_mode STRING,
    sub_mode STRING,
    exercise STRING,
    users INT,
    user_exercises INT,
    problems INT,
    correct INT,
    profs INT,
    prof_prob_count INT,
    first_attempts INT,
    hint_probs INT,
    time_taken INT)
COMMENT 'Contains daily time series data for daily ex stats dashboard'
PARTITIONED BY (dt STRING)
LOCATION 's3://ka-mapreduce/summary_tables/daily_exercise_stats';
ALTER TABLE daily_exercise_stats RECOVER PARTITIONS;

--------------------------------------------------------------------------------
-- Utility files that custom Python mapper/reducer scripts can import.

-- NOTE: To import a utility file, you'll need to add the current directory to
-- Python's path before the import so Hadoop can pick it up:
--     sys.path.append(os.path.dirname(__file__))
--     import table_parser


ADD FILE s3://ka-mapreduce/code/py/table_parser.py;
