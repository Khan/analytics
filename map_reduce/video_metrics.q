-- Hive script for generating video usage metrics.
-- When the script is run, 3 parameters need to be supplied
-- pfx: table postfix for the output summary tables
-- start_dt: start date stamp YYYY-mm-dd
-- end_dt: end date stamp YYYY-mm-dd
--
-- Example for running the metrics for April: 
-- hive -d INPATH=s3://ka-mapreduce/entity_store \
-- -i s3://ka-mapreduce/code/hive/ka_hive_init.q \
-- -d pfx=apr -d start_dt=2012-04-01 -d end_dt=2012-04-30 \
-- -f s3://ka-mapreduce/code/hive/video_metrics.q


-- Summary usage by topic
DROP TABLE topic_summary_registered_${pfx};
CREATE EXTERNAL TABLE topic_summary_registered_${pfx}(
topic STRING, registered INT,
users INT, visits INT, completed INT, seconds INT)
LOCATION 's3://ka-mapreduce/tmp/topic_summary_registered_${pfx}';

INSERT OVERWRITE TABLE topic_summary_registered_${pfx}
SELECT video_topic.topic_title,
IF(userdata_ids.user_id RLIKE 'nouserid' OR 
  (userdata_ids.user_id IS NULL AND userdata_ids.user RLIKE 'nouserid'), 0, 1), 
COUNT(distinct user_video_summary.user), 
COUNT(distinct concat(user_video_summary.user, user_video_summary.dt)),
SUM(user_video_summary.completed), SUM(user_video_summary.num_seconds)
FROM user_video_summary LEFT OUTER JOIN userdata_ids ON 
(user_video_summary.user = userdata_ids.user)
JOIN video_topic ON (video_topic.vid_key = user_video_summary.video_key)
WHERE user_video_summary.dt >= '${start_dt}' AND 
user_video_summary.dt <= '${end_dt}'
GROUP BY video_topic.topic_title,
IF(userdata_ids.user_id RLIKE 'nouserid' OR 
  (userdata_ids.user_id IS NULL AND userdata_ids.user RLIKE 'nouserid'), 0, 1);


-- Total Usage
INSERT INTO TABLE topic_summary_registered_${pfx}
SELECT 'total',
IF(userdata_ids.user_id RLIKE 'nouserid' OR 
  (userdata_ids.user_id IS NULL AND userdata_ids.user RLIKE 'nouserid'), 0, 1),
COUNT(distinct user_video_summary.user), 
COUNT(distinct concat(user_video_summary.user, user_video_summary.dt)),
SUM(user_video_summary.completed), SUM(user_video_summary.num_seconds)
FROM user_video_summary LEFT OUTER JOIN userdata_ids ON 
(user_video_summary.user = userdata_ids.user)
WHERE user_video_summary.dt >= '${start_dt}' AND 
user_video_summary.dt <= '${end_dt}'
GROUP BY 'total',
IF(userdata_ids.user_id RLIKE 'nouserid' OR
   (userdata_ids.user_id IS NULL AND userdata_ids.user RLIKE 'nouserid'), 0, 1);


-- Summarize the video usage by user
DROP TABLE user_video_summary_${pfx};
CREATE EXTERNAL TABLE user_video_summary_${pfx}(
  user STRING, visits STRING, videos int, completed int, seconds int)
LOCATION 's3://ka-mapreduce/tmp/user_video_summary_${pfx}'; 

INSERT OVERWRITE TABLE user_video_summary_${pfx}
SELECT user, COUNT(distinct dt), COUNT(distinct video_key), 
  SUM(completed), SUM(num_seconds)
FROM user_video_summary WHERE 
  dt >= '${start_dt}' AND dt <= '${end_dt}'
GROUP BY user;


-- Some high level metrics on usage distribution
DROP TABLE user_video_distribution_${pfx};
CREATE EXTERNAL TABLE user_video_distribution_${pfx}(
  registered INT, visits INT, num_users INT, 
  videos INT, completed INT, seconds INT) 
LOCATION 's3://ka-mapreduce/tmp/user_video_distrbution_${pfx}' ;

INSERT OVERWRITE TABLE user_video_distribution_${pfx} 
SELECT IF(userdata_ids.user_id RLIKE 'nouserid' OR
  (userdata_ids.user_id IS NULL AND userdata_ids.user RLIKE 'nouserid'), 0, 1),
visits, COUNT(1), SUM(videos), SUM(completed), SUM(seconds) 
FROM user_video_summary_${pfx} LEFT OUTER JOIN userdata_ids ON
  (user_video_summary_${pfx}.user = userdata_ids.user)
GROUP BY IF(userdata_ids.user_id RLIKE 'nouserid' OR
  (userdata_ids.user_id is null and userdata_ids.user RLIKE 'nouserid'), 0, 1),
  visits;
