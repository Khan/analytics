-- Hive script for running the reducer that gives accuracy deltas
-- between known test cards, and average them across each user segment and
-- ending card in topic.
-- Required script arguments:
-- start_dt: The first day of the range of attempts to reduce over as YYYY-MM-DD
-- end_dt: The exclusive end date (one past the last day) of the range of
--     attempts to reduce over as YYYY-MM-DD
-- suffix: Suffix to append to the generated table name to label the table.

ADD FILE s3://ka-mapreduce/code/py/accuracy_deltas_reducer.py;

-- TODO(david): This should be put into a more permanent table so data can be
--     regularly loaded into mongo for display.
DROP TABLE accuracy_deltas_${suffix};
CREATE EXTERNAL TABLE accuracy_deltas_${suffix} (
    user_segment STRING, num_problems_done INT, card_number INT,
    sum_deltas DOUBLE, num_deltas INT, avg_deltas DOUBLE)
  COMMENT 'Average accuracy deltas across user segments and ending cards'
  LOCATION 's3://ka-mapreduce/tmp/accuracy_deltas';

INSERT OVERWRITE TABLE accuracy_deltas_${suffix}
  SELECT user_segment, num_problems_done, card_number, SUM(reduce_out.delta),
    COUNT(*), AVG(reduce_out.delta)
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
    AS user_segment, num_problems_done, card_number, delta
  ) reduce_out
  GROUP BY user_segment, num_problems_done, card_number;
