-- Hive script for filling a partition of topic_attempts table from
-- a join of stacklog cards and problemlog on problem log keys.
-- Required script arguments:
-- dt: day of problem logs to summarize as YYYY-MM-DD

ADD FILE s3://ka-mapreduce/code/py/stacklog_cards_mapper.py;
ADD FILE s3://ka-mapreduce/code/hive/create_topic_attempts.q;
SOURCE /mnt/var/lib/hive_0110/downloaded_resources/create_topic_attempts.q;

CREATE TABLE IF NOT EXISTS topic_attempts_temp LIKE topic_attempts;

-- Daily job summarizing attempts in topic mode by joining problem logs with
-- cards in stack logs.
-- We only need to do an inner join because we only need to insert new rows for
-- problems the user did today (while recent stack logs may have cards from days
-- ago) in topic mode.
INSERT OVERWRITE TABLE topic_attempts_temp PARTITION (dt='${dt}')
SELECT DISTINCT
  stacktable.user, stacktable.topic, problemtable.exercise,
  problemtable.time_done, problemtable.time_taken,
  problemtable.problem_number, problemtable.correct,
  stacktable.scheduler_info, stacktable.user_segment
FROM (
  FROM problemlog
    SELECT
      get_json_object(problemlog.json, '$.key') AS key,
      get_json_object(problemlog.json, '$.exercise') AS exercise,
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

-- Create a pseudo-topic "any" for easy aggregation of data for all topics. This
-- takes the union of all the attempt data with a copy of all the attempt data
-- but with topic set to "any". (Apparently INSERT INTO does not append to
  -- a partition.)
-- TODO(david): Perhaps this should be a view to save space?
INSERT OVERWRITE TABLE topic_attempts PARTITION (dt='${dt}')
SELECT * FROM (
    SELECT
      user, topic, exercise, time_done, time_taken, problem_number,
      correct, scheduler_info, user_segment
    FROM topic_attempts_temp
    WHERE dt='${dt}'
  UNION ALL
    SELECT
      user, 'any' as topic, exercise, time_done, time_taken, problem_number,
      correct, scheduler_info, user_segment
    FROM topic_attempts_temp
    WHERE dt='${dt}'
) union_result;

DROP TABLE topic_attempts_temp;


-- TODO(david): Figure out why we can't use the more efficient json_tuple below
--     as a subquery in the first join clause above. Error is parse error about
--     `fields` (but query works alright by itself).
-- (SELECT fields.* FROM problemlog
--  LATERAL VIEW
--    json_tuple(problemlog.json, 'key', 'time_done', 'correct',
--      'problem_number')
--  fields AS f1, f2, f3, f4
-- WHERE problemlog.dt = '2012-06-10') problemtable
