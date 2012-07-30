-- For every day a user was active, summarize those activities.
-- This script first creates a temporary staging table with the results
-- over the entire range of dates.  That staged result is then inserted into 
-- dynamic partitions.  This 2-step process is done for performance, and gave
-- a 50x boost compared to running a jobflow step per day/partition when 
-- backfilling over a large date range.
--
-- Required arguments: start_dt and end_dt to specify a date range.

DROP TABLE user_daily_activity_staged;
CREATE EXTERNAL TABLE IF NOT EXISTS user_daily_activity_staged(
  user STRING,
  dt STRING,
  joined BOOLEAN,
  feedback_items INT,
  videos_started INT, videos_completed INT, videos_seconds INT,
  exercises_started INT, exercises_completed INT, 
  exercises_problems_done INT, exercises_seconds INT)
LOCATION 's3://ka-mapreduce/tmp/user_daily_activity_staged';


-- TODO(jace):  get rid of UNION ALL verbosity, do a cleaner join
INSERT OVERWRITE TABLE user_daily_activity_staged
SELECT
  a.user,
  a.dt,
  SUM(a.joined) > 0,
  SUM(a.feedback_items),
  SUM(a.videos_started), SUM(a.videos_completed), SUM(a.videos_seconds),
  SUM(a.exercises_started), SUM(a.exercises_completed),
  SUM(a.exercises_problems_done), SUM(a.exercises_seconds)
FROM (
  
  SELECT 
    user,
    dt,
    0 AS joined,
    COUNT(1) AS feedback_items,
    0 AS videos_started, 
    0 AS videos_completed,
    0 AS videos_seconds,
    0 AS exercises_started,
    0 AS exercises_completed,
    0 AS exercises_problems_done,
    0 AS exercises_seconds
  FROM user_feedback_summary
  WHERE dt >= '${start_dt}' AND dt < '${end_dt}'
  GROUP BY user, dt

  UNION ALL

  SELECT 
    user,
    dt,
    0 AS joined,
    0 AS feedback_items,
    COUNT(1) AS videos_started, 
    SUM(IF(completed, 1, 0)) AS videos_completed,
    SUM(num_seconds) AS videos_seconds,
    0 AS exercises_started,
    0 AS exercises_completed,
    0 AS exercises_problems_done,
    0 AS exercises_seconds
  FROM user_video_summary
  WHERE dt >= '${start_dt}' AND dt < '${end_dt}'
  GROUP BY user, dt

  UNION ALL
  
  SELECT 
    user,
    dt,
    0 AS joined,
    0 AS feedback_items,
    0 AS videos_started, 
    0 AS videos_completed,
    0 AS videos_seconds,
    COUNT(1) AS exercises_started, 
    SUM(IF(proficient, 1, 0)) AS exercises_completed,
    SUM(num_correct + num_wrong) AS exercises_problems_done,
    SUM(time_spent) AS exercises_seconds
  FROM user_exercise_summary
  WHERE dt >= '${start_dt}' AND dt < '${end_dt}'
  GROUP BY user, dt

  UNION ALL
  
  SELECT 
    user,
    to_date(from_unixtime(floor(userdata_info.joined))) AS dt,
    1 AS joined,
    0 AS feedback_items,
    0 AS videos_started, 
    0 AS videos_completed,
    0 AS videos_seconds,
    0 AS exercises_started,
    0 AS exercises_completed,
    0 AS exercises_problems_done,
    0 AS exercises_seconds
  FROM userdata_info
  WHERE to_date(from_unixtime(floor(userdata_info.joined))) >= '${start_dt}' AND
        to_date(from_unixtime(floor(userdata_info.joined))) < '${end_dt}' AND
        registered = TRUE

) a
GROUP BY a.dt, a.user;



-- now insert the temporary/staging table into dynamic partions
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET mapred.reduce.tasks=128;
INSERT OVERWRITE TABLE user_daily_activity PARTITION(dt)
SELECT user, joined, feedback_items,
       videos_started, videos_completed, videos_seconds,
       exercises_started, exercises_completed,
       exercises_problems_done, exercises_seconds,
       dt
FROM user_daily_activity_staged
DISTRIBUTE BY dt;
SET mapred.reduce.tasks=-1;

