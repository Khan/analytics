-- Query to populate exercise_summary table as defined in ka_hive_init.
-- Records correct, wrong attempts as well as time taken for an exercise.
-- Since Perseus exercises do not declare problem type in their case
--  the grouping is done by seed value.

-- Perseus (static questions) exercise can be differentiated
--  by having non integer seed.
-- TODO(robert): Make sure to update it if anything changes

-- Refer to exercise_prof_summary.q for percent proficient of an exercise.

-- Required script arguments:
-- start_dt: The first day of the range of problem attempts to
--     reduce over as YYYY-MM-DD
-- end_dt: The exclusive end date (one past the last day) of the range of
--     attempts to reduce over as YYYY-MM-DD

DROP TABLE IF EXISTS exercise_summary_staged;
CREATE EXTERNAL TABLE IF NOT EXISTS exercise_summary_staged (
    exercise STRING,
    sub_exercise_type STRING,
    correct_attempts INT,
    wrong_attempts INT,
    time_taken INT,
    is_static BOOLEAN,
    dt STRING
  )
LOCATION 's3://ka-mapreduce/tmp/exercise_summary_staged';

-- Query all non perseus exercises
INSERT OVERWRITE TABLE exercise_summary_staged
SELECT
  parsed.exercise, parsed.sub_exercise_type,
  SUM(parsed.correct), SUM(parsed.wrong),
  SUM(parsed.time_taken), parsed.is_static,
  parsed.dt
FROM (
  SELECT
    get_json_object(ProblemLog.json, '$.exercise') AS exercise,
    get_json_object(ProblemLog.json, '$.problem_type') AS sub_exercise_type,
    IF(get_json_object(ProblemLog.json, '$.correct') = "true", 1, 0) AS correct,
    IF(get_json_object(ProblemLog.json, '$.correct') != "true", 1, 0) AS wrong,
    IF(get_json_object(ProblemLog.json, '$.time_taken') > 600, 600,
        IF(get_json_object(ProblemLog.json, '$.time_taken') < 0, 0,
        get_json_object(ProblemLog.json, '$.time_taken'))) AS time_taken,
    cast(
        -- We end up deriving this information client side anyways
        -- Useful for future consumers of these summaries
        get_json_object(ProblemLog.json, '$.seed') AS INT) IS NULL AS is_static,
    ProblemLog.dt
  FROM ProblemLog
  WHERE ProblemLog.dt >= '${start_dt}' AND ProblemLog.dt < '${end_dt}' AND
    -- Perseus exercise can be differentiated by having non integer seed.
    cast(get_json_object(ProblemLog.json, '$.seed') AS INT) IS NOT NULL
) parsed
GROUP BY parsed.exercise, parsed.sub_exercise_type, parsed.is_static, parsed.dt;

-- Query all perseus exercises
INSERT INTO TABLE exercise_summary_staged
SELECT
  parsed.exercise, parsed.sub_exercise_type,
  SUM(parsed.correct), SUM(parsed.wrong),
  SUM(parsed.time_taken), parsed.is_static,
 parsed.dt
FROM (
  SELECT
    get_json_object(ProblemLog.json, '$.exercise') AS exercise,
    get_json_object(ProblemLog.json, '$.seed') AS sub_exercise_type,
    IF(get_json_object(ProblemLog.json, '$.correct') = "true", 1, 0) AS correct,
    IF(get_json_object(ProblemLog.json, '$.correct') != "true", 1, 0) AS wrong,
    IF(get_json_object(ProblemLog.json, '$.time_taken') > 600, 600,
        IF(get_json_object(ProblemLog.json, '$.time_taken') < 0, 0,
        get_json_object(ProblemLog.json, '$.time_taken'))) AS time_taken,
    cast(
        -- We end up deriving this information client side anyways
        -- Useful for future consumers of these summaries
        get_json_object(ProblemLog.json, '$.seed') AS INT) IS NULL AS is_static,
    ProblemLog.dt
  FROM ProblemLog
  WHERE ProblemLog.dt >= '${start_dt}' AND ProblemLog.dt < '${end_dt}' AND
    -- Perseus exercise can be differentiated by having non integer seed.
    cast(get_json_object(ProblemLog.json, '$.seed') AS INT) IS NULL
) parsed
GROUP BY parsed.exercise, parsed.sub_exercise_type, parsed.is_static, parsed.dt;

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET mapred.reduce.tasks=128;

INSERT OVERWRITE TABLE exercise_summary PARTITION(dt)
SELECT exercise, sub_exercise_type,
       correct_attempts, wrong_attempts,
       time_taken, is_static, dt
FROM exercise_summary_staged
DISTRIBUTE BY dt;

SET mapred.reduce.tasks=-1;
