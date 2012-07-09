-- Hive script for generating video usage metrics.
-- Outputs will be written to tables partitioned by a string representing
-- the date range specified.
--
-- Arguments:
--   path_prefix: The S3 path, with trailing slash, of where the output
--      table should live. Must be non-empty!
--   start_dt: start date stamp YYYY-mm-dd
--   end_dt: exclusive end date stamp YYYY-mm-dd
--
-- Example for running the metrics for April:
-- hive -i s3://ka-mapreduce/code/hive/ka_hive_init.q \
-- -d INPATH=s3://ka-mapreduce/entity_store \
-- -d path_prefix=summary_tables/reports/videos/ \
-- -d start_dt=2012-04-01 -d end_dt=2012-05-01 \
-- -f s3://ka-mapreduce/code/hive/video_metrics.q


DROP TABLE topic_summary_registered;
CREATE EXTERNAL TABLE topic_summary_registered(
  topic STRING, registered BOOLEAN,
  users INT, visits INT, completed INT, seconds INT)
PARTITIONED BY (start_dt STRING, end_dt STRING)
LOCATION 's3://ka-mapreduce/${path_prefix}topic_summary_registered';
ALTER TABLE topic_summary_registered RECOVER PARTITIONS;

-- Summary usage by topic
ALTER TABLE topic_summary_registered
    DROP PARTITION (start_dt='${start_dt}', end_dt='${end_dt}');

-- INSERT INTO will overwrite the partition contents.
INSERT INTO TABLE topic_summary_registered
PARTITION (start_dt='${start_dt}', end_dt='${end_dt}')
SELECT * FROM (
    SELECT
      video_topic.topic_title,
      userdata_info.registered,
      COUNT(distinct user_video_summary.user),
      COUNT(distinct concat(user_video_summary.user, user_video_summary.dt)),
      SUM(IF(user_video_summary.completed, 1, 0)),
      SUM(user_video_summary.num_seconds)
    FROM user_video_summary
    LEFT OUTER JOIN userdata_info ON
      (user_video_summary.user = userdata_info.user)
    JOIN video_topic ON
      (video_topic.vid_key = user_video_summary.video_key)
    WHERE user_video_summary.dt >= '${start_dt}' AND
      user_video_summary.dt < '${end_dt}'
    GROUP BY video_topic.topic_title, userdata_info.registered
  UNION ALL
    -- Total Usage (single row - fakes out topic title with 'total')
    SELECT
      'total' as topic_title,
      userdata_info.registered,
      COUNT(distinct user_video_summary.user),
      COUNT(distinct concat(user_video_summary.user, user_video_summary.dt)),
      SUM(IF(user_video_summary.completed, 1, 0)),
      SUM(user_video_summary.num_seconds)
    FROM user_video_summary LEFT OUTER JOIN userdata_info ON
      (user_video_summary.user = userdata_info.user)
    WHERE user_video_summary.dt >= '${start_dt}' AND
      user_video_summary.dt < '${end_dt}'
    GROUP BY 'total', userdata_info.registered
) unionresult;


-- Summarize the video usage by user
DROP TABLE user_video_range_summary;
CREATE EXTERNAL TABLE user_video_range_summary(
  user STRING, visits STRING, videos INT, completed INT, seconds INT)
PARTITIONED BY (start_dt STRING, end_dt STRING)
LOCATION 's3://ka-mapreduce/${path_prefix}user_video_range_summary';
ALTER TABLE user_video_range_summary RECOVER PARTITIONS;

ALTER TABLE user_video_range_summary
    DROP PARTITION (start_dt='${start_dt}', end_dt='${end_dt}');

INSERT OVERWRITE TABLE user_video_range_summary
PARTITION (start_dt='${start_dt}', end_dt='${end_dt}')
SELECT user, COUNT(distinct dt), COUNT(distinct video_key),
  SUM(IF(completed, 1, 0)), SUM(num_seconds)
FROM user_video_summary WHERE
  dt >= '${start_dt}' AND dt < '${end_dt}'
GROUP BY user;

-- Some high level metrics on usage distribution
DROP TABLE user_video_distribution;
CREATE EXTERNAL TABLE user_video_distribution(
  registered BOOLEAN, visits INT, num_users INT,
  videos INT, completed INT, seconds INT)
PARTITIONED BY (start_dt STRING, end_dt STRING)
LOCATION 's3://ka-mapreduce/${path_prefix}user_video_distribution';
ALTER TABLE user_video_distribution RECOVER PARTITIONS;

ALTER TABLE user_video_distribution
    DROP PARTITION (start_dt='${start_dt}', end_dt='${end_dt}');

INSERT OVERWRITE TABLE user_video_distribution
PARTITION (start_dt='${start_dt}', end_dt='${end_dt}')
SELECT
  userdata_info.registered,
  visits, COUNT(1),
  SUM(videos), SUM(completed), SUM(seconds)
FROM user_video_range_summary LEFT OUTER JOIN userdata_info ON
  (user_video_range_summary.user = userdata_info.user)
WHERE
  user_video_range_summary.start_dt = '${start_dt}' AND
  user_video_range_summary.end_dt = '${end_dt}'
GROUP BY userdata_info.registered, visits;
