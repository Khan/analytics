-- This query answers the question:
-- "Based on their students' involvement, which are the most active coaches?"

-- The input parameters are start_dt and end_dt.

-- The output includes, for each coach:
-- Their email or identifying string
-- Geographical information, such as their city and state.
-- The sum of various involvement metrics over their students.

-- This file assumes that the following tables have already been computed
-- via ka_hive_init.q:
-- user_coach_mapping

-- The only way I tested this script was copy+pasting it into a hive
-- console that had been launched with this command:
-- /home/hadoop/bin/hive -i s3://ka-mapreduce/code/hive/ka_hive_init.q -d INPATH=s3://ka-mapreduce/entity_store
-- You can set the start_dt and end_dt parameters below.

-- We assume the result of this table already exists in S3, and we can just
-- mount it.
-- Contains one row per student/coach mapping.  One row may have self_coach
-- set to true, if this person has themselves set as a coach.
CREATE EXTERNAL TABLE IF NOT EXISTS user_coach_mapping(
  user STRING,
  coach STRING,
  self_coach BOOLEAN
) LOCATION 's3://ka-mapreduce/tmp/user_coach_mapping';

-- Emit one row per student/teacher, for each teacher who has >= 10 students.
CREATE EXTERNAL TABLE student_teacher_mapping(
    student STRING,
    teacher STRING
) LOCATION 's3://ka-mapreduce/tmp/student_teacher_mapping';

INSERT OVERWRITE TABLE student_teacher_mapping
SELECT ucm.user, ucm.coach
FROM user_coach_mapping ucm
JOIN coach_summary cs
    ON ucm.coach = cs.coach
WHERE cs.num_students >= 10;

-- TODO(mattfaus): Add an age column

-- All coach/student activity within a window
CREATE EXTERNAL TABLE class_windowed_activity(
  user STRING,
  feedback_items INT,
  videos_started INT,
  videos_completed INT,
  videos_seconds INT,
  exercises_started INT,
  exercises_completed INT,
  exercises_problems_done INT,
  exercises_seconds INT
) LOCATION 's3://ka-mapreduce/tmp/class_windowed_activity_1308_1405';

--
--
-- TODO(mattfaus): Change this to 2012-08-01
--
--

set hivevar:start_dt='2013-08-01';
set hivevar:end_dt='2014-05-31';

-- 2013/10/22 class_windowed_activity was created for 2012-08-15 to 2013-11-01
-- Saved at 's3://ka-mapreduce/tmp/class_windowed_activity'
-- 2013/11/06 class_windowed_activity was created for 2013-08-01 to 2014-05-31
-- Saved at 's3://ka-mapreduce/tmp/class_windowed_activity_1308_1405'

INSERT OVERWRITE TABLE class_windowed_activity
-- Hive only supports equi-joins (can't OR the conditions together) and
-- you can only UNION subqueries together.
SELECT * FROM (
  SELECT uda.user,
      SUM(uda.feedback_items),
      SUM(uda.videos_started),
      SUM(uda.videos_completed),
      SUM(uda.videos_seconds),
      SUM(uda.exercises_started),
      SUM(uda.exercises_completed),
      SUM(uda.exercises_problems_done),
      SUM(uda.exercises_seconds)
  FROM student_teacher_mapping stm
  JOIN user_daily_activity uda
  ON stm.student = uda.user
  -- Set start and end dates here
  WHERE uda.dt >= ${start_dt}
        AND uda.dt < ${end_dt}
  GROUP BY uda.user

  UNION ALL

  SELECT uda.user,
      SUM(uda.feedback_items),
      SUM(uda.videos_started),
      SUM(uda.videos_completed),
      SUM(uda.videos_seconds),
      SUM(uda.exercises_started),
      SUM(uda.exercises_completed),
      SUM(uda.exercises_problems_done),
      SUM(uda.exercises_seconds)
  FROM student_teacher_mapping stm
  JOIN user_daily_activity uda
  ON stm.teacher = uda.user
  -- Set start and end dates here
  WHERE uda.dt >= ${start_dt}
        AND uda.dt < ${end_dt}
  GROUP BY uda.user
) subquery;


-- Get the UserEvent for coach's clicking on the reports dashboard

-- Find all CoachReportLoadEvent
SELECT
  uej.user_key, count(*)
FROM UserEvent ue
  -- TODO(mattfaus): Why doesn't class[1] extract just the sub-class name?
LATERAL VIEW JSON_TUPLE(ue.json, 'user_key', 'class') uej AS user_key, class
  -- TODO(mattfaus): Extract dates to parameters
  -- and make sure they match the other queries
  -- SET hivevar:userdata_partition=2013-10-14;
WHERE ue.dt >= ${start_dt}
  AND ue.dt < ${end_dt}
  and instr(uej.class, "CoachReportLoadEvent") > 0
group by uej.user_key;

-- TODO(mattfaus):
-- Join this to UserData.key to aggregate how many times each user has
-- viewed a coach report over the time period

--feedback_items  videos_started  videos_completed

-- This table finds ~38000 coaches, but joining on udi.user_email reduces it
-- to ~29000 which is why we do the left outer join
-- select *
--     JOIN UserData ud ON subquery.teacher = get_json_object(ud.json, '$.user'))
--   -- use explode(collect_set()) to get 1 row per possible identifying email?
-- FROM (subquery1)


CREATE EXTERNAL TABLE most_active_coaches(
  teacher_user STRING,
  teacher_user_email STRING,

  num_students INT,
  sum_student_feedback_items INT,
  sum_student_videos_started INT,
  sum_student_videos_completed INT,
  sum_student_video_seconds INT,
  sum_student_exercises_started INT,
  sum_student_exercises_completed INT,
  sum_student_exercise_problems_done INT,
  sum_student_exercise_seconds INT,

  -- Bucketed student activity
  -- Number of students with >1,5,15,20,25,30 hour(s) of video activity
  students_1_hour_video_time INT,
  students_5_hour_video_time INT,
  students_10_hour_video_time INT,
  students_15_hour_video_time INT,
  students_20_hour_video_time INT,
  students_25_hour_video_time INT,
  students_30_hour_video_time INT,

  -- Number of students with >1,5,15,20,25,30 hour(s) of exercise activity
  students_1_hour_exercise_time INT,
  students_5_hour_exercise_time INT,
  students_10_hour_exercise_time INT,
  students_15_hour_exercise_time INT,
  students_20_hour_exercise_time INT,
  students_25_hour_exercise_time INT,
  students_30_hour_exercise_time INT,

  -- Teacher activity
  sum_teacher_feedback_items INT,
  sum_teacher_videos_started INT,
  sum_teacher_videos_completed INT,
  sum_teacher_video_seconds INT,
  sum_teacher_exercises_started INT,
  sum_teacher_exercises_completed INT,
  sum_teacher_exercise_problems_done INT,
  sum_teacher_exercise_secon INT
) LOCATION 's3://ka-mapreduce/tmp/most_active_coaches';

INSERT OVERWRITE TABLE most_active_coaches
SELECT
  -- Create 1 row with each of the teacher IDs
  -- explode(array(subquery.teacher, subquery.user_email)) as teacher_ID,
  -- Maureen's VLOOKUP uses the user_email column if it is available, otherwise
  -- fals back to the "user" column, so maybe we could just do that logic here?
  subquery.teacher, -- "user"
  subquery.user_email, -- "user_email"

  -- These SUM()'s will mainly just be combining the actual result with 0
  -- from the two subqueries - one which aggregates the students, and one for the teacher
  SUM(subquery.num_students),
  SUM(subquery.sum_student_feedback_items),
  SUM(subquery.sum_student_videos_started),
  SUM(subquery.sum_student_videos_completed),
  SUM(subquery.sum_student_video_seconds),
  SUM(subquery.sum_student_exercises_started),
  SUM(subquery.sum_student_exercises_completed),
  SUM(subquery.sum_student_exercise_problems_done),
  SUM(subquery.sum_student_exercise_seconds),

  -- Bucketed student activity
  -- Number of students with >1,5,15,20,25,30 hour(s) of video activity
  SUM(subquery.students_1_hour_video_time),
  SUM(subquery.students_5_hour_video_time),
  SUM(subquery.students_10_hour_video_time),
  SUM(subquery.students_15_hour_video_time),
  SUM(subquery.students_20_hour_video_time),
  SUM(subquery.students_25_hour_video_time),
  SUM(subquery.students_30_hour_video_time),

  -- Number of students with >1,5,15,20,25,30 hour(s) of exercise activity
  SUM(subquery.students_1_hour_exercise_time),
  SUM(subquery.students_5_hour_exercise_time),
  SUM(subquery.students_10_hour_exercise_time),
  SUM(subquery.students_15_hour_exercise_time),
  SUM(subquery.students_20_hour_exercise_time),
  SUM(subquery.students_25_hour_exercise_time),
  SUM(subquery.students_30_hour_exercise_time),

  -- Teacher activity
  SUM(subquery.sum_teacher_feedback_items),
  SUM(subquery.sum_teacher_videos_started),
  SUM(subquery.sum_teacher_videos_completed),
  SUM(subquery.sum_teacher_video_seconds),
  SUM(subquery.sum_teacher_exercises_started),
  SUM(subquery.sum_teacher_exercises_completed),
  SUM(subquery.sum_teacher_exercise_problems_done),
  SUM(subquery.sum_teacher_exercise_seconds)

FROM (
  -- Query to find the sum of all student activity
  SELECT stm.teacher as teacher,
      udi.user_email as user_email,
      COUNT(*) as num_students,
      -- TODO(mattfaus): Add udi.auth_emails and udi.user_data_key
      SUM(swa.feedback_items) as sum_student_feedback_items,
      SUM(swa.videos_started) as sum_student_videos_started,
      SUM(swa.videos_completed) as sum_student_videos_completed,
      SUM(swa.videos_seconds) AS sum_student_video_seconds,
      SUM(swa.exercises_started) as sum_student_exercises_started,
      SUM(swa.exercises_completed) as sum_student_exercises_completed,
      SUM(swa.exercises_problems_done) as sum_student_exercise_problems_done,
      SUM(swa.exercises_seconds) AS sum_student_exercise_seconds,

      -- Bucketed student activity
      -- Number of students with >1,5,15,20,25,30 hour(s) of video activity
      SUM(IF((swa.videos_seconds > 60 * 60 * 1), 1, 0)) as students_1_hour_video_time,
      SUM(IF((swa.videos_seconds > 60 * 60 * 5), 1, 0)) as students_5_hour_video_time,
      SUM(IF((swa.videos_seconds > 60 * 60 * 10), 1, 0)) as students_10_hour_video_time,
      SUM(IF((swa.videos_seconds > 60 * 60 * 15), 1, 0)) as students_15_hour_video_time,
      SUM(IF((swa.videos_seconds > 60 * 60 * 20), 1, 0)) as students_20_hour_video_time,
      SUM(IF((swa.videos_seconds > 60 * 60 * 25), 1, 0)) as students_25_hour_video_time,
      SUM(IF((swa.videos_seconds > 60 * 60 * 30), 1, 0)) as students_30_hour_video_time,

      -- Number of students with >1,5,15,20,25,30 hour(s) of exercise activity
      SUM(IF((swa.exercises_seconds > 60 * 60 * 1), 1, 0)) as students_1_hour_exercise_time,
      SUM(IF((swa.exercises_seconds > 60 * 60 * 5), 1, 0)) as students_5_hour_exercise_time,
      SUM(IF((swa.exercises_seconds > 60 * 60 * 10), 1, 0)) as students_10_hour_exercise_time,
      SUM(IF((swa.exercises_seconds > 60 * 60 * 15), 1, 0)) as students_15_hour_exercise_time,
      SUM(IF((swa.exercises_seconds > 60 * 60 * 20), 1, 0)) as students_20_hour_exercise_time,
      SUM(IF((swa.exercises_seconds > 60 * 60 * 25), 1, 0)) as students_25_hour_exercise_time,
      SUM(IF((swa.exercises_seconds > 60 * 60 * 30), 1, 0)) as students_30_hour_exercise_time,

      -- Teacher aggregations
      0 as sum_teacher_feedback_items,
      0 as sum_teacher_videos_started,
      0 as sum_teacher_videos_completed,
      0 AS sum_teacher_video_seconds,
      0 as sum_teacher_exercises_started,
      0 as sum_teacher_exercises_completed,
      0 as sum_teacher_exercise_problems_done,
      0 AS sum_teacher_exercise_seconds

  FROM student_teacher_mapping stm
  JOIN class_windowed_activity swa
  ON stm.student = swa.user
  LEFT OUTER JOIN userdata_info udi
  ON stm.teacher = udi.user
  GROUP BY stm.teacher, udi.user_email

  UNION ALL

  -- Query to find the teacher's activity
  SELECT stm.teacher as teacher,
      udi.user_email as user_email,
      0 as num_students,
      -- TODO(mattfaus): Add udi.auth_emails and udi.user_data_key
      0 as sum_student_feedback_items,
      0 as sum_student_videos_started,
      0 as sum_student_videos_completed,
      0 AS sum_student_video_seconds,
      0 as sum_student_exercises_started,
      0 as sum_student_exercises_completed,
      0 as sum_student_exercise_problems_done,
      0 AS sum_student_exercise_seconds,

      -- Bucketed student activity
      -- Number of students with >1,5,15,20,25,30 hour(s) of video activity
      0 as students_1_hour_video_time,
      0 as students_5_hour_video_time,
      0 as students_10_hour_video_time,
      0 as students_15_hour_video_time,
      0 as students_20_hour_video_time,
      0 as students_25_hour_video_time,
      0 as students_30_hour_video_time,

      -- Number of students with >1,5,15,20,25,30 hour(s) of exercise activity
      0 as students_1_hour_exercise_time,
      0 as students_5_hour_exercise_time,
      0 as students_10_hour_exercise_time,
      0 as students_15_hour_exercise_time,
      0 as students_20_hour_exercise_time,
      0 as students_25_hour_exercise_time,
      0 as students_30_hour_exercise_time,

      -- Teacher aggregations
      SUM(swa.feedback_items) as sum_teacher_feedback_items,
      SUM(swa.videos_started) as sum_teacher_videos_started,
      SUM(swa.videos_completed) as sum_teacher_videos_completed,
      SUM(swa.videos_seconds) AS sum_teacher_video_seconds,
      SUM(swa.exercises_started) as sum_teacher_exercises_started,
      SUM(swa.exercises_completed) as sum_teacher_exercises_completed,
      SUM(swa.exercises_problems_done) as sum_teacher_exercise_problems_done,
      SUM(swa.exercises_seconds) AS sum_teacher_exercise_seconds

  FROM student_teacher_mapping stm
  JOIN class_windowed_activity swa
  -- This join condition is the main thing that changed from the previous query
  ON stm.teacher = swa.user
  LEFT OUTER JOIN userdata_info udi
  ON stm.teacher = udi.user
  GROUP BY stm.teacher, udi.user_email

) subquery
GROUP BY subquery.teacher, subquery.user_email;


INSERT OVERWRITE LOCAL DIRECTORY '/tmp/most_active_coaches'
SELECT * FROM most_active_coaches;

-- TODO(mattfaus): Join most_active_coaches with student_teacher_mapping again
-- get the birthdate for each student from UserData.json.get_json_tuple("$.birthdate")
-- AVG() over these (but compute the age from the birthdate in years)
-- SELECT

INSERT OVERWRITE LOCAL DIRECTORY '/tmp/coach_student_avg_age'
SELECT
  stm.teacher, AVG(datediff(
    from_unixtime(unix_timestamp()),
    from_unixtime(cast(cast(get_json_object(ud.json, "$.birthdate") as DECIMAL) as BIGINT))
  ) / 365.25),
  STDDEV_POP(datediff(
    from_unixtime(unix_timestamp()),
    from_unixtime(cast(cast(get_json_object(ud.json, "$.birthdate") as DECIMAL) as BIGINT))
  ) / 365.25),
  COUNT(*)
FROM student_teacher_mapping stm
LEFT OUTER JOIN UserData ud
ON stm.student = get_json_object(ud.json, "$.user")
WHERE get_json_object(ud.json, "$.birthdate") <> "null"
GROUP BY stm.teacher;


SELECT
  (datediff(
    from_unixtime(unix_timestamp()),
    from_unixtime(cast(cast(get_json_object(ud.json, "$.birthdate") as DECIMAL) as BIGINT))
  ) / 365.25)
FROM UserData ud
WHERE get_json_object(ud.json, "$.birthdate") <> "null"
LIMIT 100;


-- TODO(mattfaus): Download a IP<->Organization database and join this to ip_address on ProblemLog
-- http://www.maxmind.com/en/organization

-- Convert the above file to csv-format with:
-- cat -v /tmp/most_active_coaches/* | sed 's/,/_/g' | sed 's/\^A/,/g' > most_active_coaches-2013-01_10.csv

-- Copy this locally with something like:
-- elastic-mapreduce -j j-G85HR9YTY3GJ --get /home/hadoop/most_active_coaches-2012-08_2013-11.csv


-- TODO(mattfaus): Add the geographical information back into the query above?
-- Issue query, writing results to a local file, as shown here:
-- http://stackoverflow.com/questions/18129581/how-do-i-output-the-results-of-a-hiveql-query-to-csv
INSERT OVERWRITE LOCAL DIRECTORY '/tmp/most_active_coaches'
SELECT stm.teacher,
    tc.student_count,
    tc.user_id,
    tc.joined,
    tc.ip,
    tc.city,
    tc.region,
    tc.country_code,
    tc.country,
    tc.latitude,
    tc.longitude,
    SUM(swa.feedback_items),
    SUM(swa.videos_started),
    SUM(swa.videos_completed),
    SUM(swa.videos_seconds) AS sum_video_seconds,
    SUM(swa.exercises_started),
    SUM(swa.exercises_completed),
    SUM(swa.exercises_problems_done),
    -- TODO(mattfaus): Move this logic into user_exercise_summary.q, for parity
    -- with user_video_summary.q
    SUM(IF(swa.exercises_seconds < 0,  -- Clamp in [0, 600) seconds for sanity
         0,
         IF(swa.exercises_seconds > 600,
            600,
            swa.exercises_seconds
      ))) AS sum_exercise_seconds
FROM student_teacher_mapping stm
JOIN class_windowed_activity swa
ON stm.student = swa.user
JOIN teacher_country tc
ON stm.teacher = tc.teacher
GROUP BY stm.teacher,
    tc.student_count,
    tc.user_id,
    tc.joined,
    tc.ip,
    tc.city,
    tc.region,
    tc.country_code,
    tc.country,
    tc.latitude,
    tc.longitude
ORDER BY sum_video_seconds, sum_exercise_seconds;

