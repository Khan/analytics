-- This Hive script is used to generate a training dataset for accuracy models.

-- Required script arguments:
-- start_dt: The first day of the range of attempts to reduce over as YYYY-MM-DD
-- end_dt: The exclusive end date (one past the last day) of the range of
--     attempts to reduce over as YYYY-MM-DD
-- suffix: Suffix to append to the generated table name to label the table.
-- branch: name (directory) of code branch in s3 to use in some ADD statements

DROP TABLE accuracy_featureset_${suffix};
CREATE EXTERNAL TABLE accuracy_featureset_${suffix} (
    correct INT,
    forecast DOUBLE,
    topic STRING,
    exercise STRING,
    problem_number INT,
    topic_problem_number INT,
    ewma_3 DOUBLE,
    ewma_10 DOUBLE,
    streak DOUBLE,
    log_num_done DOUBLE,
    log_num_missed DOUBLE,
    percent_correct DOUBLE,
    T DOUBLE,
    E DOUBLE
  )
  COMMENT 'Accuracy model featureset for model training'
  LOCATION 's3://ka-mapreduce/tmp/accuracy_featureset';

ADD FILE s3://ka-mapreduce/code/${branch}/py/accuracy_model_featureset.py;
ADD FILE s3://ka-mapreduce/code/${branch}/py/accuracy_model_baseline.py;
ADD FILE s3://ka-mapreduce/resource/topic_net_models.json;

INSERT OVERWRITE TABLE accuracy_featureset_${suffix}
SELECT featureset.*
FROM (
  FROM (
    FROM topic_attempts
    SELECT *
    WHERE dt >= '${start_dt}' AND dt < '${end_dt}'
    DISTRIBUTE BY user, topic
    SORT BY user, topic, time_done
  ) attempts
  SELECT TRANSFORM(attempts.*)
  USING 'python accuracy_model_featureset.py -s nth -f 10'
  AS correct, forecast, topic, exercise,
    problem_number, topic_problem_number,
    ewma_3, ewma_10, streak, log_num_done,
    log_num_missed, percent_correct, T, E
) featureset;

