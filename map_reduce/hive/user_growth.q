-- Based user_daily_activity data, compute daily time series of the deltas 
-- in user growth, aggregated to daily, weekly, and monthly levels.
-- The output of the user_growth.py reducer and the resulting user_growth
-- consists of three columns: 
--     dt - the date for the data point.
--     series - the name of the account status change being counted.  
--              examples include signups, deactiviations, and reactivations.
--     value - the # ("delta") of account transitions for the specified
--             (dt, series).  This value is always positive.
-- Required script arguments: start_dt and end_dt to specify a date range.
-- Note that even though this script requires a start_dt and end_dt, the 
-- expected case is to summarize a complete (or nearly complete) website 
-- history, because user_growth.py has stateful logic and expectst to have
-- each user account's entire history.  start_dt should be used as the 
-- earliest possible date of valide user data, and end_dt is the as-of
-- date of this report's generation.

ADD FILE s3://ka-mapreduce/code/py/user_growth.py;

INSERT OVERWRITE TABLE user_growth PARTITION (timescale='daily')
SELECT deltas.dt, deltas.series, SUM(deltas.value)
FROM (
  FROM (
    SELECT a.user, a.dt, a.joined
    FROM user_daily_activity a
    JOIN userdata_info b
    ON a.user = b.user
    WHERE b.registered
    DISTRIBUTE BY user
    SORT BY user, dt
  ) activity
  SELECT TRANSFORM(activity.*)
  USING 'python user_growth.py ${start_dt} ${end_dt}'
  AS dt, series, value
) deltas
GROUP BY deltas.dt, deltas.series;

-- Aggregate the daily series up to a weekly series
INSERT OVERWRITE TABLE user_growth PARTITION (timescale='weekly')
SELECT 
  date_sub('${end_dt}', cast(datediff('${end_dt}', d.dt)/7 AS INT)*7) AS dt, 
  d.series, 
  SUM(d.value) AS value
FROM user_growth d
WHERE d.timescale='daily'
GROUP BY 
  date_sub('${end_dt}', cast(datediff('${end_dt}', d.dt)/7 AS INT)*7), 
  d.series;

-- Aggregate the daily series up to a monthly series
INSERT OVERWRITE TABLE user_growth PARTITION (timescale='monthly')
SELECT 
  date_sub('${end_dt}', cast(datediff('${end_dt}', d.dt)/28 AS INT)*28) AS dt, 
  d.series, 
  SUM(d.value) AS value
FROM user_growth d
WHERE d.timescale='daily'
GROUP BY 
  date_sub('${end_dt}', cast(datediff('${end_dt}', d.dt)/28 AS INT)*28), 
  d.series;

