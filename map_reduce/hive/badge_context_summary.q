-- Query to compute summary metrics for badges
-- Produces for each badge:
--  * Total number of times each badge has been awarded
--  * Number of unique users that have given badge
-- Keeps data per badge context value to allow for more detailed analysis

-- Arguments:
--  start_dt: inclusive begin date in YYYY-MM-DD format
--  end_dt: exclusive end date in YYYY-MM-DD format

DROP TABLE IF EXISTS badge_context_unique_count;
CREATE EXTERNAL TABLE IF NOT EXISTS badge_context_unique_count (
  badge_name STRING,
  context_name STRING,
  unique_awarded INT,
  dt STRING
)  LOCATION 's3://ka-mapreduce/tmp/badge_context_unique_count';

DROP TABLE IF EXISTS badge_context_total_count;
CREATE EXTERNAL TABLE IF NOT EXISTS badge_context_total_count (
  badge_name STRING,
  context_name STRING,
  total_awarded INT,
  total_points_earned INT,
  dt STRING
)  LOCATION 's3://ka-mapreduce/tmp/badge_context_total_count';

DROP TABLE IF EXISTS badge_context_summary_staged;
CREATE EXTERNAL TABLE IF NOT EXISTS badge_context_summary_staged (
  badge_name STRING,
  context_name STRING,
  total_awarded INT,
  unique_awarded INT,
  total_points_earned INT,
  dt STRING
)  LOCATION 's3://ka-mapreduce/tmp/badge_context_summary_staged';

INSERT OVERWRITE TABLE badge_context_total_count
SELECT
  j.badge_name,
  IF(j.context_name IS NULL, "None", j.context_name),
  COUNT(1) AS total_awarded,
  SUM(j.points_earned) AS total_points_earned,
  b.dt
FROM UserBadge b
-- Use json_tuple instead of get_json_object for performance gains
-- https://cwiki.apache.org/Hive/languagemanual-udf.html#LanguageManualUDF-jsontuple
LATERAL VIEW json_tuple(b.json, "badge_name", "target_context_name",
    "points_earned") j AS badge_name, context_name, points_earned
WHERE b.dt >= '${start_dt}' AND b.dt < '${end_dt}'
GROUP BY j.badge_name, j.context_name, b.dt;

INSERT OVERWRITE TABLE badge_context_unique_count
SELECT
  ubq.badge_name, ubq.context_name,
  COUNT(1) as unique_awarded,
  ubq.dt
FROM (
  SELECT
    j.badge_name,
    IF(j.context_name IS NULL, "None", j.context_name) AS context_name,
    j.user, MIN(ub.dt) AS dt
  FROM UserBadge ub
  LATERAL VIEW json_tuple(ub.json, "badge_name", "target_context_name",
    "user") j AS badge_name, context_name, user
  GROUP BY j.badge_name, j.context_name, j.user
) ubq
GROUP BY ubq.badge_name, ubq.context_name, ubq.dt;

INSERT OVERWRITE TABLE badge_context_summary_staged
SELECT
  u.badge_name, u.context_name,
  u.total_awarded, uq.unique_awarded,
  u.total_points_earned, u.dt
FROM badge_context_total_count u
LEFT OUTER JOIN badge_context_unique_count uq
ON u.dt = uq.dt
  AND u.badge_name = uq.badge_name
  AND u.context_name = uq.context_name;

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET mapred.reduce.tasks=128;

INSERT OVERWRITE TABLE badge_context_summary PARTITION(dt)
SELECT
  b.badge_name, b.context_name,
  b.total_awarded, b.unique_awarded,
  b.total_points_earned, b.dt
FROM badge_context_summary_staged b
DISTRIBUTE BY dt;

SET mapred.reduce.tasks=-1;
