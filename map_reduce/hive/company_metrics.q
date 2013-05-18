-- This script computes 3 of the 4 company-wide growth metrics for
-- the ranges given by ${start_dt} and ${end_dt}.  This script outputs numbers
-- on a month-level granularity, so ${start_dt} should always be the first day
-- of a month.  This script also overwrites the months for which it computes
-- data, so be sure to always run this script for months that are completed
-- (in addition to the current month).

-- As of 2013-05-17, this script takes ~25min to populate the company_metrics
-- table in Hive, and an additional ~1min to push that data from Hive to MongoDB

-- For the definition of the company_metrics table, see ka_hive_init

-- Note: Metric 1 (Unique visitors) is easily pulled from Google analytics
-- (but not from Hive) and thus is not computed here.

-- We must enable dynamic partitions
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-- This uses "dynamic partion" writes based on the value of activity_month
-- NOTE, the order of columns must be the same that comes back with describe company_metrics
INSERT OVERWRITE TABLE company_metrics PARTITION (activity_month)
SELECT
  sum(activity_by_user_month.registered_this_month) as registrations_this_month,
  sum(activity_by_user_month.long_term) as long_term_users_active_this_month,
  sum(activity_by_user_month.highly_engaged) as highly_engaged_users_active_this_month,
  activity_by_user_month.activity_month as activity_month

FROM (
  SELECT

    activity_by_user_day.activity_month as activity_month,

    -- Each user only counts towards 1 registration month
    -- (max is only needed here to avoid "not included in the GROUP BY" errors)
    CASE WHEN (max(activity_by_user_day.joined_month) = activity_by_user_day.activity_month)
      THEN 1
      ELSE 0
    END as registered_this_month,

    -- A user may or may not be counted as a "long term user" for each month.
    -- Once a user becomes "long term" they will be counted for every month
    -- that they log at least some activity (because this context is grouped by month)
    CASE WHEN (
        -- Pick the activity furthest from registration within this month
        -- (max is only needed here to avoid "not included in the GROUP BY" errors)
        datediff(max(activity_by_user_day.activity_dt),

        -- And compare that against registration
        -- (max is only needed here to avoid "not included in the GROUP BY" errors)
        max(activity_by_user_day.joined_dt))

        -- If the activity happend more than 28 days from registration, this user is long-term!
        >= 28)
      THEN 1
      ELSE 0
    END as long_term,

    -- A user may or may not be highly engaged for any given month after their registration
    CASE WHEN (count(*) >= 4)
      THEN 1
      ELSE 0
    END as highly_engaged

  FROM (

        SELECT
          userdata_info.user as user,
          from_unixtime(floor(userdata_info.joined)) as joined_dt,
          substr(from_unixtime(floor(userdata_info.joined)), 1, 7) as joined_month,
          user_daily_activity.dt as activity_dt,
          substr(user_daily_activity.dt, 1, 7) AS activity_month

        FROM user_daily_activity
        JOIN userdata_info
          ON user_daily_activity.user = userdata_info.user
        WHERE
              -- Note that we don't filter userdata_info.joined. We need to
              -- pick up all of the users, because they may need to be
              -- counted as long_term or highly_engaged
          userdata_info.registered
          and user_daily_activity.dt >= '${start_dt}'
          and user_daily_activity.dt < '${end_dt}'

        ) activity_by_user_day

  GROUP BY activity_by_user_day.user, activity_by_user_day.activity_month

) activity_by_user_month
GROUP BY activity_by_user_month.activity_month
ORDER BY activity_month;

-- Reset Hive configuration options
set hive.exec.dynamic.partition=false;
set hive.exec.dynamic.partition.mode=strict;
