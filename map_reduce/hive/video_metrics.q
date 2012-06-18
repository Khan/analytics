-- Hive script for generating video usage metrics.
-- When the script is run, 3 parameters need to be supplied
-- suffix: table postfix for the output summary tables
-- start_dt: start date stamp YYYY-mm-dd
-- end_dt: exclusive end date stamp YYYY-mm-dd
--
-- Example for running the metrics for April:
-- hive -d INPATH=s3://ka-mapreduce/entity_store \
-- -i s3://ka-mapreduce/code/hive/ka_hive_init.q \
-- -d suffix=apr -d start_dt=2012-04-01 -d end_dt=2012-05-01 \
-- -f s3://ka-mapreduce/code/hive/video_metrics.q


-- Summary usage by topic
DROP TABLE topic_summary_registered_${suffix};
CREATE EXTERNAL TABLE topic_summary_registered_${suffix}(
  topic STRING, registered INT,
  users INT, visits INT, completed INT, seconds INT)
LOCATION 's3://ka-mapreduce/tmp/topic_summary_registered_${suffix}';

INSERT OVERWRITE TABLE topic_summary_registered_${suffix}
SELECT video_topic.topic_title,
  userdata_info.registered,
  COUNT(distinct user_video_summary.user),
  COUNT(distinct concat(user_video_summary.user, user_video_summary.dt)),
  SUM(user_video_summary.completed), SUM(user_video_summary.num_seconds)
FROM user_video_summary LEFT OUTER JOIN userdata_info ON
  (user_video_summary.user = userdata_info.user)
JOIN video_topic ON (video_topic.vid_key = user_video_summary.video_key)
WHERE user_video_summary.dt >= '${start_dt}' AND
  user_video_summary.dt < '${end_dt}'
GROUP BY video_topic.topic_title, userdata_info.registered;


-- Total Usage
INSERT INTO TABLE topic_summary_registered_${suffix}
SELECT 'total',
  userdata_info.registered,
  COUNT(distinct user_video_summary.user),
  COUNT(distinct concat(user_video_summary.user, user_video_summary.dt)),
  SUM(user_video_summary.completed), SUM(user_video_summary.num_seconds)
FROM user_video_summary LEFT OUTER JOIN userdata_info ON
  (user_video_summary.user = userdata_info.user)
WHERE user_video_summary.dt >= '${start_dt}' AND
  user_video_summary.dt < '${end_dt}'
GROUP BY 'total', userdata_info.registered;


-- Summarize the video usage by user
DROP TABLE user_video_summary_${suffix};
CREATE EXTERNAL TABLE user_video_summary_${suffix}(
  user STRING, visits STRING, videos int, completed int, seconds int)
LOCATION 's3://ka-mapreduce/tmp/user_video_summary_${suffix}';

INSERT OVERWRITE TABLE user_video_summary_${suffix}
SELECT user, COUNT(distinct dt), COUNT(distinct video_key),
  SUM(completed), SUM(num_seconds)
FROM user_video_summary WHERE
  dt >= '${start_dt}' AND dt < '${end_dt}'
GROUP BY user;


-- Some high level metrics on usage distribution
DROP TABLE user_video_distribution_${suffix};
CREATE EXTERNAL TABLE user_video_distribution_${suffix}(
  registered INT, visits INT, num_users INT,
  videos INT, completed INT, seconds INT)
LOCATION 's3://ka-mapreduce/tmp/user_video_distrbution_${suffix}' ;

INSERT OVERWRITE TABLE user_video_distribution_${suffix}
SELECT userdata_info.registered,
  visits, COUNT(1), SUM(videos), SUM(completed), SUM(seconds)
FROM user_video_summary_${suffix} LEFT OUTER JOIN userdata_info ON
  (user_video_summary_${suffix}.user = userdata_info.user)
GROUP BY userdata_info.registered, visits;
