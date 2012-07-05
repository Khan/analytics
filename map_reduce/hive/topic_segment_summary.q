-- Hive script for aggregating supplemental exercise data for every user segment
-- for each exercise topic.
--
-- Required script arguments:
-- start_dt: The first day of the range of attempts to reduce over as YYYY-MM-DD
-- end_dt: The exclusive end date (one past the last day) of the range of
--     attempts to reduce over as YYYY-MM-DD

-- TODO(david): Run reducers serially in a cron job
-- TODO(david): Context switching summary: preceding, interleaving, and
--     succeeding time spent on other things
-- TODO(david): There is a huge amount of code replication in this file!
--     SQL/Hive experts, would be glad to learn of better ways of writing this!
-- TODO(david): Use a map-side join?


DROP TABLE topic_segment_summary;
CREATE EXTERNAL TABLE IF NOT EXISTS topic_segment_summary (
  topic STRING, user_segment STRING, all_users INT, all_attempts INT,
  total_time INT, random_card_users INT, first_card_correct_all INT,
  first_card_attempts_all INT, first_card_correct_randomized INT,
  first_card_attempts_randomized INT, num_correct_all INT, num_attempts_all INT,
  num_correct_randomized INT, num_attempts_randomized INT,
  sum_deltas_randomized DOUBLE, num_deltas_randomized INT
) COMMENT 'Supplemental exercise data for each topic-segment'
PARTITIONED BY (start_dt STRING, end_dt STRING)
LOCATION 's3://ka-mapreduce/summary_tables/topic_segment_summary';

DROP VIEW topic_attempts_p;
CREATE VIEW topic_attempts_p AS
SELECT * FROM topic_attempts
WHERE dt >= '${start_dt}' AND dt < '${end_dt}';

DROP VIEW topic_retention_summary_p;
CREATE VIEW topic_retention_summary_p AS
SELECT * FROM topic_retention_summary
WHERE start_dt = '${start_dt}' AND end_dt = '${end_dt}';

DROP VIEW accuracy_deltas_summary_p;
CREATE VIEW accuracy_deltas_summary_p AS
SELECT * FROM accuracy_deltas_summary
WHERE start_dt = '${start_dt}' AND end_dt = '${end_dt}';


INSERT OVERWRITE TABLE topic_segment_summary
PARTITION (start_dt='${start_dt}', end_dt='${end_dt}')
SELECT t1.topic, t1.user_segment, t1.all_users, t1.all_attempts, t1.total_time,
  t2.random_card_users,
  t3.first_card_correct_all, t3.first_card_attempts_all,
  t4.first_card_correct_randomized, t4.first_card_attempts_randomized,
  t5.num_correct_all, t5.num_attempts_all,
  t6.num_correct_randomized, t6.num_attempts_randomized,
  t7.sum_deltas_randomized, t7.num_deltas_randomized

FROM (
  -- Total # of users, # of attempts, total time taken
  SELECT topic, user_segment, COUNT(DISTINCT user) AS all_users,
    COUNT(*) AS all_attempts, SUM(time_taken) AS total_time
  FROM topic_attempts_p
  GROUP BY topic, user_segment
) t1

JOIN (
  -- # of randomized card users
  SELECT topic, user_segment, COUNT(DISTINCT user) AS random_card_users
  FROM topic_attempts_p
  WHERE get_json_object(topic_attempts_p.scheduler_info, '$.purpose') ==
    'randomized'
  GROUP BY topic, user_segment
) t2 ON (t1.topic = t2.topic AND t1.user_segment = t2.user_segment)

JOIN (
  -- First card % correct
  SELECT topic, user_segment, SUM(num_correct) AS first_card_correct_all,
    SUM(num_attempts) AS first_card_attempts_all
  FROM topic_retention_summary_p
  WHERE bucket_type = 'card_number' AND bucket_value = 1
  GROUP BY topic, user_segment
) t3 ON (t1.topic = t3.topic AND t1.user_segment = t3.user_segment)

JOIN (
  -- First card % correct for randomized cards
  SELECT topic, user_segment, SUM(num_correct) AS first_card_correct_randomized,
    SUM(num_attempts) AS first_card_attempts_randomized
  FROM topic_retention_summary_p
  WHERE bucket_type = 'card_number' AND bucket_value = 1 AND
    is_randomized = true
  GROUP BY topic, user_segment
) t4 ON (t1.topic = t4.topic AND t1.user_segment = t4.user_segment)

JOIN (
  -- All cards % correct
  SELECT topic, user_segment, SUM(num_correct) AS num_correct_all,
    SUM(num_attempts) AS num_attempts_all
  FROM topic_retention_summary_p
  WHERE bucket_type = 'card_number'
  GROUP BY topic, user_segment
) t5 ON (t1.topic = t5.topic AND t1.user_segment = t5.user_segment)

JOIN (
  -- Randomized cards % correct
  SELECT topic, user_segment, SUM(num_correct) AS num_correct_randomized,
    SUM(num_attempts) AS num_attempts_randomized
  FROM topic_retention_summary_p
  WHERE bucket_type = 'card_number' AND is_randomized = TRUE
  GROUP BY topic, user_segment
) t6 ON (t1.topic = t6.topic AND t1.user_segment = t6.user_segment)

JOIN (
  -- Total accuracy gains
  SELECT topic, user_segment, SUM(sum_deltas) AS sum_deltas_randomized,
    SUM(num_deltas) AS num_deltas_randomized
  FROM accuracy_deltas_summary_p
  GROUP BY topic, user_segment
) t7 ON (t1.topic = t7.topic AND t1.user_segment = t7.user_segment)

;
