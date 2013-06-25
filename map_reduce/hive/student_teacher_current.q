-- Common part of teachers and students metrics
-- Finds first day for each coach, user when he became a teacher, student

DROP TABLE IF EXISTS user_coach_date;
CREATE EXTERNAL TABLE IF NOT EXISTS user_coach_date(
  user STRING,
  coach STRING,
  gae_bingo_identity STRING,
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
  teacher STRING,
  dt STRING
) LOCATION 's3://ka-mapreduce/tmp/student_on_date';

ADD FILE s3://ka-mapreduce/code/py/coach_reduce.py;
ADD FILE s3://ka-mapreduce/code/py/ka_udf.py;

-- Extract relevant information from UserData table
-- bingo_identity is used to extract data from website request logs
INSERT OVERWRITE TABLE user_coach_date
SELECT a.user, a.coach, a.gae_bingo_identity,
  from_unixtime(cast(cast(a.joined AS FLOAT) AS INT), 'yyyy-MM-dd')
    AS joined_on,
  (a.coach = a.user or a.coach = a.user_email or
   a.coach = a.user_id) as self_coach
FROM (
  SELECT TRANSFORM(UserData.json)
  USING 'ka_udf.py explode user,user_id,user_email,joined,gae_bingo_identity coaches'
  AS user, user_id, user_email, joined, gae_bingo_identity, coach
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
USING 'coach_reduce.py teacher 10' AS teacher, dt;

-- Find first date when each user, coach pair became student, teacher pair
INSERT OVERWRITE TABLE student_on_date
SELECT u_dt.user, t_dt.teacher, IF(MIN(t_dt.dt) > MIN(u_dt.joined_on),
    MIN(t_dt.dt), MIN(u_dt.joined_on)) AS dt
FROM user_coach_date u_dt
JOIN teacher_on_date t_dt
ON u_dt.coach = t_dt.teacher
WHERE NOT u_dt.self_coach
GROUP BY u_dt.user, t_dt.teacher;
