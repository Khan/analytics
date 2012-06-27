-- Hive script for aggregating retention statistics for topics over number of
-- cards done.
--
-- Required script arguments:
-- start_dt: The first day of the range of attempts to reduce over as YYYY-MM-DD
-- end_dt: The exclusive end date (one past the last day) of the range of
--     attempts to reduce over as YYYY-MM-DD

ADD FILE s3://ka-mapreduce/code/py/topic_retention_reducer.py;

CREATE EXTERNAL TABLE IF NOT EXISTS topic_retention_summary (
  topic STRING, user_segment STRING, bucket_type STRING, bucket_value INT,
  num_correct INT, num_attempts INT
) COMMENT 'User retention on exercise topics over time or number of cards done'
PARTITIONED BY (start_dt STRING, end_dt STRING)
LOCATION 's3://ka-mapreduce/summary_tables/topic_retention_summary';

-- TODO(david): There's a bit of duplicated code here between accuracy_deltas.q
--     (the initial mapper bit).
INSERT OVERWRITE TABLE topic_retention_summary
PARTITION (start_dt='${start_dt}', end_dt='${end_dt}')
SELECT topic, user_segment, bucket_type, bucket_value, SUM(correct), COUNT(*)
FROM (
  FROM (
    FROM topic_attempts
    SELECT *
    WHERE dt >= '${start_dt}' AND dt < '${end_dt}'
    DISTRIBUTE BY user, topic
    SORT BY user, topic, time_done
  ) map_output
  SELECT TRANSFORM(map_output.*)
  USING 'topic_retention_reducer.py'
  AS topic, user_segment, bucket_type, bucket_value, correct
) reduce_out
GROUP BY topic, user_segment, bucket_type, bucket_value;
