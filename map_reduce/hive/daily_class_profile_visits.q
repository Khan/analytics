-- Keeps track of bingo id's who have visited coach reports on any given day
-- Used to measure number of coaches using reports

-- Arguments:
--   end_dt: exclusive end date in YYYY-MM-DD format
--   start_dt: inclusive start date in YYYY-MM-DD format

DROP TABLE IF EXISTS daily_class_profile_visits_staged;
CREATE EXTERNAL TABLE IF NOT EXISTS daily_class_profile_visits_staged (
    bingo_id STRING,
    url STRING,
    dt STRING
) LOCATION 's3://ka-mapreduce/tmp/daily_class_profile_visits_staged';

-- According to Coach'n'Class (Josh) the "/class_profile" route is a prefix
--  to all coach dashboard routes.
-- Furthermore we cannot check for existence of "class_profile" in
--  url since login redirect
--  (/login?continue=http%3A//www.khanacademy.org/class_profile)
--  will cause false positives.
-- There are definitely more examples like
--  this, however, that's the one that have been encountered first
INSERT OVERWRITE TABLE daily_class_profile_visits_staged
SELECT w.bingo_id, w.url, w.dt
FROM website_request_logs w
WHERE w.dt >= "${start_dt}" AND w.dt < "${end_dt}" AND
-- If you're wondering why there is 2 not 1 below it's because
--  strings in hive are 1 indexed.
-- We ignore the leading "/" for readability. Hence, we take substring
--  from 2nd position
  (substr(w.url, 2, 13) = "class_profile" OR
-- Coach Reports refactor changes path from /class_profile to /coach/reports
  substr(w.url, 2, 13) = "coach/reports");


SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET mapred.reduce.tasks=128;

INSERT OVERWRITE TABLE daily_class_profile_visits PARTITION(dt)
SELECT
  t.bingo_id, t.url, t.dt
FROM daily_class_profile_visits_staged t
DISTRIBUTE BY dt;

SET mapred.reduce.tasks=-1;
