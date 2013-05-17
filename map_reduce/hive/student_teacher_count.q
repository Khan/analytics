-- Query to produce time series of students and teachers using the site.
-- TODO(robert): Results produced by these queries are fundamentally flawed.
-- We have to record date of change of coach to be able to produce
--  accurate report

-- Arguments:
--   end_dt: exclusive end date in YYYY-MM-DD format

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

DROP TABLE IF EXISTS student_on_date;
CREATE EXTERNAL TABLE IF NOT EXISTS student_on_date(
  student STRING,
  dt STRING
) LOCATION 's3://ka-mapreduce/tmp/student_on_date';

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

-- Custom map reduce routine
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
USING 'coach_reduce.py teacher "${end_dt}"' AS teacher, dt;

-- Custom map reduce routine
-- Since we already have a list of teachers we can use it to
--  find students
-- refer to map_reduce/py/coach_reduce.py for reduce function
FROM (
    SELECT u_dt.user, MAX(t_dt.dt, u_dt.joined_on)
    FROM user_coach_date u_dt
    JOIN teacher_on_date t_dt
    ON u_dt.coach = t_dt.teacher
    WHERE NOT u_dt.self_coach
    DISTRIBUTE BY u_dt.user
    SORT BY u_dt.user DESC, t_dt.dt ASC
) st_date
INSERT OVERWRITE TABLE student_on_date
REDUCE st_date.user, st_date.dt
USING 'coach_reduce.py student "${end_dt}"' AS student, dt;

INSERT OVERWRITE TABLE student_teacher_count
SELECT t_nr.teacher_count, st_nr.student_count, t_nr.dt
FROM (
    SELECT COUNT(1) as teacher_count, dt
    FROM teacher_on_date
    GROUP BY dt
) t_nr
JOIN (
    SELECT COUNT(1) as student_count, dt
    FROM student_on_date
    GROUP BY dt
) st_nr
ON t_nr.dt = st_nr.dt;
