-- Query to compute summary metrics for badges
-- Produces for each  badge:
--  * Total number of times each badge has been awared
--  * Number of unique awards of each badge
--      (number of users that have given badge)
-- Keeps data per badge context value to allow for more detailed analysis

-- Arguments:
--  start_dt: inclusive begin date in YYYY-MM-DD format
--  end_dt: exclusive end date in YYYY-MM-DD format

DROP TABLE IF EXISTS user_badge;
CREATE EXTERNAL TABLE IF NOT EXISTS user_badge (
  badge_name STRING,
  context_name STRING,
  user STRING,
  points_earned INT,
  times_earned INT,
  dt STRING
) LOCATION 's3://ka-mapreduce/tmp/user_badge';

DROP TABLE IF EXISTS badge_summary_staged;
CREATE EXTERNAL TABLE IF NOT EXISTS badge_summary_staged (
  badge_name STRING,
  context_name STRING,
  total_awarded INT,
  unique_awarded INT,
  total_points_earned INT,
  dt STRING
)  LOCATION 's3://ka-mapreduce/tmp/badge_summary_staged';

INSERT OVERWRITE TABLE user_badge
SELECT
  j.badge_name, j.context_name, j.user,
  SUM(j.points_earned), COUNT(1) AS times_earned, b.dt
FROM UserBadge b
-- Use json_tuple instead of get_json_object for performance gains
-- https://cwiki.apache.org/Hive/languagemanual-udf.html#LanguageManualUDF-jsontuple
LATERAL VIEW json_tuple(b.json, "badge_name", "target_context_name",
    "user", "points_earned") j AS badge_name,
    context_name, user, points_earned
WHERE b.dt >= '${start_dt}' AND b.dt < '${end_dt}'
GROUP BY j.badge_name, j.context_name, j.user, b.dt;

INSERT OVERWRITE TABLE badge_summary_staged
SELECT
  user_badge.badge_name, user_badge.context_name,
  SUM(user_badge.times_earned) AS total_awarded,
  COUNT(1) AS unique_awarded,
  SUM(user_badge.points_earned) AS total_points_earned,
  user_badge.dt
FROM user_badge
GROUP BY user_badge.badge_name, user_badge.context_name, user_badge.dt;

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET mapred.reduce.tasks=128;

INSERT OVERWRITE TABLE badge_summary PARTITION(dt)
SELECT
  b.badge_name, b.context_name,
  b.total_awarded, b.unique_awarded,
  b.total_points_earned, b.dt
FROM badge_summary_staged b
DISTRIBUTE BY dt;

SET mapred.reduce.tasks=-1;
