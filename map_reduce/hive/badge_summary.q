-- Query to compute summary metrics for badges
-- Produces for each  badge:
--  * Total number of times each badge has been awarded
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

DROP TABLE IF EXISTS user_badge_unique;
CREATE EXTERNAL TABLE IF NOT EXISTS user_badge_unique (
  badge_name STRING,
  context_name STRING,
  user STRING,
  dt STRING
) LOCATION 's3://ka-mapreduce/tmp/user_badge_unique';

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

INSERT OVERWRITE TABLE user_badge_unique
SELECT
  j.badge_name, j.context_name, j.user, MIN(u.dt)
FROM UserBadge u
-- Use json_tuple instead of get_json_object for performance gains
-- https://cwiki.apache.org/Hive/languagemanual-udf.html#LanguageManualUDF-jsontuple
LATERAL VIEW json_tuple(u.json, "badge_name", "target_context_name",
    "user") j AS badge_name, context_name, user
GROUP BY j.badge_name, j.context_name, j.user;

INSERT OVERWRITE TABLE badge_summary_staged
SELECT
  u.badge_name, u.context_name,
  u.total_awarded, uq.unique_awarded,
  u.total_points_earned, u.dt
FROM (
  SELECT
    ub.badge_name, ub.context_name,
    SUM(ub.times_earned) AS total_awarded,
    SUM(ub.points_earned) AS total_points_earned,
    ub.dt
  FROM user_badge ub
  GROUP BY ub.badge_name, ub.context_name, ub.dt
) u
LEFT OUTER JOIN (
  SELECT
    ubq.badge_name, ubq.context_name, ubq.dt, COUNT(1) as unique_awarded
  FROM user_badge_unique ubq
  GROUP BY ubq.badge_name, ubq.context_name, ubq.dt
) uq
ON u.dt = uq.dt AND
   u.badge_name = uq.badge_name AND
   u.context_name = uq.context_name;

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
