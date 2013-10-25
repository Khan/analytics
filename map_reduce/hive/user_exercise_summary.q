-- Hive script for summarizing the ProblemLog daily by user, exercise
-- 1 parameter needs to be supplied
-- dt: datestamp to summarize this thing

INSERT OVERWRITE TABLE user_exercise_summary PARTITION (dt='${dt}')
SELECT
  parsed.user,
  parsed.exercise,
  SUM(IF(parsed.time_taken < 0, -- Clamp in [0, 600) for sanity
    0,
    IF(parsed.time_taken > 600,
      600,
      parsed.time_taken
    ))),
  SUM(parsed.correct),
  SUM(parsed.wrong),
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
