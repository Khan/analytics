-- Hive script for running the reducer that gives accuracy deltas
-- between known test cards, and average them across each user segment and
-- ending card in topic. Data will be stored in the partition labelled by the
-- given date range.
-- Required script arguments:
-- start_dt: The first day of the range of attempts to reduce over as YYYY-MM-DD
-- end_dt: The exclusive end date (one past the last day) of the range of
--     attempts to reduce over as YYYY-MM-DD

ADD FILE s3://ka-mapreduce/code/py/accuracy_deltas_reducer.py;
ADD FILE s3://ka-mapreduce/code/py/table_parser.py;

-- This table is defined in ka_hive_init.q
INSERT OVERWRITE TABLE accuracy_deltas_summary
  PARTITION (start_dt='${start_dt}', end_dt='${end_dt}')
  SELECT topic, user_segment, num_problems_done, card_number,
    SUM(reduce_out.delta), COUNT(*), AVG(reduce_out.delta)
  FROM (
    FROM (
      FROM topic_attempts
      SELECT *
      WHERE dt >= '${start_dt}' AND dt < '${end_dt}'
      DISTRIBUTE BY user, topic
      SORT BY user, topic, time_done
    ) map_output
    SELECT TRANSFORM(map_output.*)
    USING 'accuracy_deltas_reducer.py'
    AS topic, user_segment, num_problems_done, card_number, delta
  ) reduce_out
  GROUP BY topic, user_segment, num_problems_done, card_number;
