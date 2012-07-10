-- Hive script for summarizing the ProblemLog daily by user, exercise
-- 1 parameter needs to be supplied
-- dt: datestamp to summarize this thing

DROP TABLE user_exercise_summary;
CREATE EXTERNAL TABLE user_exercise_summary(
  user STRING, exercise STRING, time_spent  INT,
  num_correct INT, num_wrong INT, proficient BOOLEAN) PARTITIONED BY (dt STRING)
LOCATION 's3://ka-mapreduce/summary_tables/user_exercise_summary';
ALTER TABLE user_exercise_summary RECOVER PARTITIONS;


INSERT OVERWRITE TABLE user_exercise_summary PARTITION (dt='${dt}')
SELECT
  parsed.user, parsed.exercise, SUM(parsed.time_taken), 
  SUM(parsed.correct), SUM(parsed.wrong),
  MAX(IF(parsed.proficient, 1, 0)) = 1
FROM (
  SELECT
    get_json_object(ProblemLog.json, '$.user') AS user,
    get_json_object(ProblemLog.json, '$.exercise') AS exercise,
    get_json_object(ProblemLog.json, '$.time_taken') AS time_taken,
    IF(get_json_object(ProblemLog.json, '$.correct') = "true", 1, 0) AS correct,
    IF(get_json_object(ProblemLog.json, '$.correct') != "true", 1, 0) AS wrong, 
    get_json_object(ProblemLog.json, '$.earned_proficiency') = "true" 
      AS proficient
  FROM ProblemLog
  WHERE ProblemLog.dt = '${dt}'
) parsed
GROUP BY parsed.user, parsed.exercise;
