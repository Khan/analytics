-- Query to populate exercise_proficiency_summary table
--  as defined in ka_hive_init.
-- The result is number of proficiencies earned
--  but only users who had their first attempt in
--  last 6 months are counted

-- Since earned_proficiency can be True more than once for given
-- (user, exercise) pair we have to cleverly cap the number
-- in order to avoid double counting.

-- Required script arguments:
-- end_dt: The exclusive end date (one past the last day) of the range of
--     attempts to reduce over as YYYY-MM-DD

INSERT OVERWRITE TABLE exercise_proficiency_summary
SELECT
    profResult.exercise, SUM(profResult.proficient), COUNT(*)
FROM (
    SELECT
      prof.user, prof.exercise, MIN(prof.dt) AS dt, IF(SUM(prof.proficient) > 0, 1, 0) AS proficient
    FROM (
      SELECT
        get_json_object(ProblemLog.json, '$.exercise') AS exercise,
        ProblemLog.user,
        IF(get_json_object(ProblemLog.json, '$.earned_proficiency') = "true", 1, 0)
          AS proficient,
        ProblemLog.dt
      FROM ProblemLog
    ) prof
    GROUP BY prof.exercise, prof.user
) profResult
    WHERE profResult.dt >= DATE_SUB('${end_dt}', 180) AND profResult.dt < '${end_dt}'
GROUP BY profResult.exercise;
