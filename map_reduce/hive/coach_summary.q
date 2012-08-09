-- Hive script to compute coach related info


-- Temporary table for holding user_coach_mapping
CREATE EXTERNAL TABLE user_coach_mapping(
  user STRING, 
  coach STRING,
  self_coach BOOLEAN
) LOCATION 's3://ka-mapreduce/tmp/user_coach_mapping'; 

ADD FILE s3://ka-mapreduce/code/py/ka_udf.py;
INSERT OVERWRITE TABLE user_coach_mapping
SELECT a.user, a.coach, 
  (a.coach = a.user or a.coach = a.user_email or 
   a.coach = a.user_id) as self_coach  
FROM (
  SELECT TRANSFORM(UserData.json)
  USING 'ka_udf.py explode user,user_id,user_email coaches'
  AS user, user_id, user_email, coach
  FROM UserData
) a; 

INSERT OVERWRITE TABLE coach_summary
SELECT a.coach, a.num_students, (a.self_coach > 0)
FROM (
  SELECT coach, 
    count(1) as num_students, 
    SUM(IF(self_coach = true, 1, 0)) as self_coach
  FROM user_coach_mapping 
  GROUP by coach
) a; 

-- Join user_coach_mapping + coach_summary together for user_coach_summary
INSERT OVERWRITE TABLE user_coach_summary
SELECT a.user, COUNT(1), MAX(b.num_students)
FROM ( 
  SELECT user, coach FROM user_coach_mapping
  WHERE self_coach = false
) a JOIN 
(
  SELECT coach, num_students 
  FROM coach_summary
) b ON (a.coach = b.coach)
GROUP BY a.user;
