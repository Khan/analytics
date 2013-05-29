-- Query to produce time series of students and teachers using the site.
-- TODO(robert): Results produced by these queries are fundamentally flawed.
-- We have to record date of change of coach to be able to produce
--  accurate report

-- Arguments:
--   end_dt: exclusive end date in YYYY-MM-DD format

-- Computed Metrics:
--   Teacher: Coach who has at least 10 students.
--   Active Teacher: Teacher with at least 10 active students.
--   Student: User who has a teacher.
--   Active Student: Student who completed an activity in last 28 days.

DROP TABLE IF EXISTS user_coach_date;
CREATE EXTERNAL TABLE IF NOT EXISTS user_coach_date(
  user STRING,
  coach STRING,
  joined_on STRING,
  self_coach BOOLEAN
) LOCATION 's3://ka-mapreduce/tmp/user_coach_date';

DROP TABLE IF EXISTS teacher_on_date;
CREATE EXTERNAL TABLE IF NOT EXISTS teacher_on_date(
  teacher STRING,
  dt STRING
) LOCATION 's3://ka-mapreduce/tmp/teacher_on_date';

DROP TABLE IF EXISTS active_teacher_on_date;
CREATE EXTERNAL TABLE IF NOT EXISTS active_teacher_on_date(
  teacher STRING,
  dt STRING
) LOCATION 's3://ka-mapreduce/tmp/active_teacher_on_date';

DROP TABLE IF EXISTS student_on_date;
CREATE EXTERNAL TABLE IF NOT EXISTS student_on_date(
  student STRING,
  teacher STRING,
  dt STRING
) LOCATION 's3://ka-mapreduce/tmp/student_on_date';

DROP TABLE IF EXISTS active_student_on_date;
CREATE EXTERNAL TABLE IF NOT EXISTS active_student_on_date(
  student STRING,
  teacher STRING,
  dt STRING
) LOCATION 's3://ka-mapreduce/tmp/active_student_on_date';

ADD FILE s3://ka-mapreduce/code/py/coach_reduce.py;
ADD FILE s3://ka-mapreduce/code/py/ka_udf.py;

-- Extract relevant information from UserData table
INSERT OVERWRITE TABLE user_coach_date
SELECT a.user, a.coach,
  from_unixtime(cast(cast(a.joined AS FLOAT) AS INT), 'yyyy-MM-dd')
    AS joined_on,
  (a.coach = a.user or a.coach = a.user_email or
   a.coach = a.user_id) as self_coach
FROM (
  SELECT TRANSFORM(UserData.json)
  USING 'ka_udf.py explode user,user_id,user_email,joined coaches'
  AS user, user_id, user_email, joined, coach
  FROM UserData
) a;

-- Custom map reduce routine - compute first day when
--  given coach became a teacher
-- refer to map_reduce/py/coach_reduce.py for reduce function
FROM (
  SELECT user, coach, joined_on
  FROM user_coach_date
  WHERE NOT self_coach
  DISTRIBUTE BY coach
  SORT BY coach DESC, joined_on ASC
) st_date
INSERT OVERWRITE TABLE teacher_on_date
REDUCE st_date.user, st_date.coach, st_date.joined_on
USING 'coach_reduce.py teacher' AS teacher, dt;

-- Find first date when each user, coach pair became student, teacher pair
INSERT OVERWRITE TABLE student_on_date
SELECT u_dt.user, t_dt.teacher, IF(MIN(t_dt.dt) > MIN(u_dt.joined_on),
    MIN(t_dt.dt), MIN(u_dt.joined_on)) AS dt
FROM user_coach_date u_dt
JOIN teacher_on_date t_dt
ON u_dt.coach = t_dt.teacher
WHERE NOT u_dt.self_coach
GROUP BY u_dt.user, t_dt.teacher;

-- Find all active students
--  Active student is a user who performed an action,
--      as defined by user_daily_activity, in last 28 days
INSERT OVERWRITE TABLE active_student_on_date
SELECT
  student_on_date.student,
  student_on_date.teacher,
  user_daily_activity.dt
FROM user_daily_activity
JOIN student_on_date
ON user_daily_activity.user = student_on_date.student
WHERE user_daily_activity.dt < '${end_dt}' AND
  user_daily_activity.dt >= student_on_date.dt
GROUP BY student_on_date.student, student_on_date.teacher,
    user_daily_activity.dt;


-- Merge all the results together
--  1st: number of teachers in total
--  2nd: number of students in total
--  3rd: number of active students
--  4th: number of active teachers
-- Refer to map_reduce/py/coach_reduce.py and beginning of
--  this file for details
INSERT OVERWRITE TABLE student_teacher_count
SELECT t_nr.teacher_count, st_nr.student_count,
    act_t.active_teachers, act_st.active_students, t_nr.dt
FROM (
  FROM (
      SELECT COUNT(1) as teacher_count, dt
      FROM teacher_on_date
      GROUP BY dt
      ORDER BY dt
  ) t_date
  REDUCE t_date.teacher_count, t_date.dt
  USING 'coach_reduce.py count "${end_dt}"' AS teacher_count, dt
) t_nr
JOIN (
    FROM (
        SELECT COUNT(1) as student_count, dt
        FROM student_on_date
        GROUP BY dt
        ORDER BY dt
    ) st_date
    REDUCE st_date.student_count, st_date.dt
    USING 'coach_reduce.py count "${end_dt}"' AS student_count, dt
) st_nr
ON t_nr.dt = st_nr.dt
JOIN (
    FROM (
        SELECT student, dt
        FROM active_student_on_date
        ORDER BY dt
    ) active_count
    REDUCE active_count.student, active_count.dt
    USING 'coach_reduce.py active-student "${end_dt}"' AS active_students, dt
) act_st
ON t_nr.dt = act_st.dt
JOIN (
    FROM (
        SELECT student, teacher, dt
        FROM active_student_on_date
        ORDER BY dt, teacher
    ) teacher_active_count
    REDUCE teacher_active_count.student,
      teacher_active_count.teacher, teacher_active_count.dt
    USING 'coach_reduce.py active-teacher "${end_dt}"' AS active_teachers, dt
) act_t
