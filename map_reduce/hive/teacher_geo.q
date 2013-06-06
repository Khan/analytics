-- Script to geolocate teachers using their ip addresses
-- Uses last 180 days of ProblemLog to get most popular ip for given user

-- There are several steps to this query
--  1st: Map users from ProblemLog to their most popular ip address
--   from last 180 days
--  2nd: Join teachers and users to find most popular ip for each
--   coach who has become a teacher before end_dt
--      2.1nd: Peform geolocation
--  3rd: Join geolocated teachers with their user information
--   for friendlier display

-- Arguments:
--   end_dt: exclusive end date in YYYY-MM-DD format

ADD FILE s3://ka-mapreduce/code/geo/GeoLiteCity.dat;

-- Files below will be moved to pygeoip/ subdirecotry
--  for use as a python module
ADD FILE s3://ka-mapreduce/code/geo/__init__.py;
ADD FILE s3://ka-mapreduce/code/geo/util.py;
ADD FILE s3://ka-mapreduce/code/geo/timezone.py;
ADD FILE s3://ka-mapreduce/code/geo/const.py;

SOURCE FILE s3://ka-mapreduce/code/hive/student_teacher_current.q;

DROP TABLE IF EXISTS user_ip;
CREATE EXTERNAL TABLE IF NOT EXISTS user_ip (
  user STRING,
  ip STRING
) LOCATION 's3://ka-mapreduce/tmp/user_ip';

DROP TABLE IF EXISTS teacher_country_staged;
CREATE EXTERNAL TABLE IF NOT EXISTS teacher_country_staged (
  teacher STRING,
  ip STRING,
  city STRING,
  region STRING,
  country_code STRING,
  country STRING,
  latitude FLOAT,
  longitude FLOAT
) LOCATION 's3://ka-mapreduce/tmp/teacher_country_staged';

-- User to ip mapping
-- Takes last 180 days of ProblemLog and
--  returns most popular ip address for each user in that period
INSERT OVERWRITE TABLE user_ip
SELECT
    user_ip_rank.user, user_ip_rank.ip
FROM (
  SELECT TRANSFORM(user_ip_count.user, user_ip_count.ip, user_ip_count.ip_count)

  -- rank entries with same 0th column (user)
  --  according to 2nd column (ip_count)
  USING 'ka_udf.py rank 0 2 DESC'
  AS (user STRING, ip STRING, ip_count INT, rank INT)
  FROM (
    SELECT
      user,
      get_json_object(problemlog.json, '$.ip_address') AS ip,
      COUNT(1) as ip_count
    FROM ProblemLog
    WHERE dt >= DATE_SUB('${end_dt}', 180)
    GROUP BY user, get_json_object(ProblemLog.json, '$.ip_address')
  ) user_ip_count
) user_ip_rank
WHERE user_ip_rank.rank = 1;

-- Create a table that lists the most commmon ip address for each teacher.
-- Uses activity of teacher's students to determine his location.
-- Since usually students working in a classroom will report as same ip
--  address the script uses most popular ip address to resolve location.
-- Other option would be to take most popular geolocation after resolution.
-- However, due to large inaccuracies produced by the solution we are using
--  I (robert) doubt it will be an improvement. (You're)
-- Furthermore there is always a problem of students doing a lot more work
--  at home than during school time which will also bias the result.
-- Also since we don't know what is the accuracy of the latitude and longitude
--  reported by the result, and it is the part of data that we use to
--  plot the results, we might be losing information by taking most popular
--  location after ip resolution.
INSERT OVERWRITE TABLE teacher_country_staged
SELECT TRANSFORM(teacher_ip_rank.teacher, teacher_ip_rank.ip)
USING 'ka_udf.py ip_to_country 1'
AS (teacher STRING, ip STRING,
  city STRING, region STRING, country_code STRING, country STRING,
  latitude FLOAT, longitude FLOAT)
FROM (
  SELECT TRANSFORM(teacher_ip_count.teacher, teacher_ip_count.ip, teacher_ip_count.ip_count)

  -- rank entries with same 0th column (teacher)
  --  according to 2nd column (ip_count)
  USING 'ka_udf.py rank 0 2 DESC'
  AS (teacher STRING, ip STRING, ip_count INT, rank INT)
  FROM (
    SELECT
      student_on_date.teacher,
      user_ip.ip,
      COUNT(1) as ip_count
    FROM student_on_date
    JOIN user_ip
    ON student_on_date.student = user_ip.user
    GROUP BY student_on_date.teacher, user_ip.ip
  ) teacher_ip_count

) teacher_ip_rank
WHERE teacher_ip_rank.rank = 1;

-- Join teacher_country_staged with userdata_info
--  to add some personal information
INSERT OVERWRITE TABLE teacher_country
SELECT d.teacher, u.user_id, u.user_email, u.user_nickname,
    u.joined, d.ip, d.city, d.region, d.country_code, d.country,
    d.latitude, d.longitude
FROM teacher_country_staged d
JOIN userdata_info u
ON d.teacher = u.user;
