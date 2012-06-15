-- Hive script for filling a partition of topic_attempts table from
-- a join of stacklog cards and problemlog on problem log keys.
-- Required script arguments:
-- dt: day of problem logs to summarize as YYYY-MM-DD

-- TODO(david): Put in crontab to be recurring daily job.

ADD FILE s3://ka-mapreduce/code/py/stacklog_cards_mapper.py;
ADD FILE s3://ka-mapreduce/code/hive/create_topic_attempts.q;

SOURCE create_topic_attempts.q;

-- Daily job summarizing attempts in topic mode by joining problem logs with
-- cards in stack logs.
-- We only need to do an inner join because we only need to insert new rows for
-- problems the user did today (while recent stack logs may have cards from days
-- ago) in topic mode.
INSERT OVERWRITE TABLE topic_attempts PARTITION (dt='${dt}')
  SELECT DISTINCT
    stacktable.user, stacktable.topic, problemtable.time_done,
    problemtable.time_taken, problemtable.problem_number, problemtable.correct,
    stacktable.scheduler_info, stacktable.user_segment
  FROM (
    FROM problemlog
      SELECT
        get_json_object(problemlog.json, '$.key') AS key,
        cast(get_json_object(problemlog.json, '$.time_done') as double)
          AS time_done,
        cast(get_json_object(problemlog.json, '$.time_taken') as int)
          AS time_taken,
        cast(get_json_object(problemlog.json, '$.problem_number') as int)
          AS problem_number,
        get_json_object(problemlog.json, '$.correct') = "true" AS correct
      WHERE problemlog.dt = '${dt}'
    ) problemtable
  JOIN (
    FROM stacklog
      SELECT TRANSFORM(user, json, dt)
      USING 'stacklog_cards_mapper.py'
      AS key, user, topic, scheduler_info, user_segment
      WHERE stacklog.dt = '${dt}'
    ) stacktable
  ON (problemtable.key = stacktable.key);


-- TODO(david): Figure out why we can't use the more efficient json_tuple below
--     as a subquery in the first join clause above. Error is parse error about
--     `fields` (but query works alright by itself).
-- (SELECT fields.* FROM problemlog
--  LATERAL VIEW
--    json_tuple(problemlog.json, 'key', 'time_done', 'correct',
--      'problem_number')
--  fields AS f1, f2, f3, f4
-- WHERE problemlog.dt = '2012-06-10') problemtable
