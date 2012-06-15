-- Create the topic_attempts table if it doesn't exist.

CREATE EXTERNAL TABLE IF NOT EXISTS topic_attempts (
    user STRING, topic STRING, exercise STRING,
    time_done DOUBLE, time_taken INT,
    problem_number INT, correct BOOLEAN, scheduler_info STRING,
    user_segment STRING)
  COMMENT 'Join of stack log topic exercise attempt cards and problem logs'
  PARTITIONED BY (dt string)
  LOCATION 's3://ka-mapreduce/summary_tables/topic_attempts';
