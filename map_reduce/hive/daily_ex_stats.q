-- Hive script used to compute stats underlying the daily ex stats dashboard.

-- Required script arguments:
-- dt: The day for which to compute stats in the format YYYY-MM-DD
-- branch: name (directory) of code branch in s3 to use in some ADD statements


DROP TABLE daily_exercise_stats_by_user;
CREATE EXTERNAL TABLE daily_exercise_stats_by_user (
    super_mode STRING,
    sub_mode STRING,
    exercise STRING,
    users INT,
    user_exercises INT,
    problems INT,
    correct INT,
    profs INT,
    prof_prob_count INT,
    first_attempts INT,
    hint_probs INT,
    time_taken INT)
PARTITIONED BY (dt STRING)
LOCATION 's3://ka-mapreduce/summary_tables/daily_ex_stats_by_user';

ADD FILE s3://ka-mapreduce/code/${branch}/py/daily_ex_stats.py;

INSERT OVERWRITE TABLE daily_exercise_stats_by_user
PARTITION (dt='${dt}')
SELECT stats.*
FROM (
  FROM (
    FROM (
      FROM userdata
      SELECT 
        get_json_object(userdata.json, '$.user') AS user, 
        "user_info" AS type, 
        json

      UNION ALL

      FROM problemlog
      SELECT user, "plog" AS type, json
      WHERE dt = '${dt}'
    ) u
    SELECT u.*
    DISTRIBUTE BY u.user
    SORT BY u.user ASC, u.type DESC
  ) user_plogs
  SELECT TRANSFORM(user_plogs.*)
  USING 'daily_ex_stats.py ${dt}'
  AS super_mode, sub_mode, exercise, users, user_exercises, problems, correct, 
     profs, prof_prob_count, first_attempts, hint_probs, time_taken
) stats;

INSERT OVERWRITE TABLE daily_exercise_stats
PARTITION (dt='${dt}')
SELECT
    super_mode,
    sub_mode,
    exercise,
    SUM(users),
    SUM(user_exercises),
    SUM(problems),
    SUM(correct),
    SUM(profs),
    SUM(prof_prob_count),
    SUM(first_attempts),
    SUM(hint_probs),
    SUM(time_taken)
FROM daily_exercise_stats_by_user
WHERE dt = '${dt}'
GROUP BY super_mode, sub_mode, exercise;
