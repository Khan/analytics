-- Required script arguments:
-- start_dt: The first day of the range of attempts to reduce over as YYYY-MM-DD
-- end_dt: The exclusive end date (one past the last day) of the range of
--     attempts to reduce over as YYYY-MM-DD
-- suffix: Suffix to append to the generated table name to label the table.
-- min_ex: the minimum number of exercises that should have data within a topic
--     before sampling 

DROP TABLE bayes_net_dataset_${suffix};
CREATE EXTERNAL TABLE bayes_net_dataset_${suffix} (
    topic STRING, json STRING)
  COMMENT 'Dataset for Bayes net training'
  LOCATION 's3://ka-mapreduce/tmp/bayes_net_dataset';

-- This defines the topics for which to create a Bayes net 
-- TODO(jace) Cron something to keep this up to date
ADD FILE s3://ka-mapreduce/resource/topic_tree.json;

ADD FILE s3://ka-mapreduce/code/py/accuracy_model_baseline.py;
ADD FILE s3://ka-mapreduce/code/py/topic_util.py;
ADD FILE s3://ka-mapreduce/code/py/bayes_net_dataset.py;

-- TODO(jace): For some reason when tried to elmintate the outermost FROM
-- clause, Hive complained about syntax.  Need to understand that.
INSERT OVERWRITE TABLE bayes_net_dataset_${suffix}
SELECT dataset.*
FROM (
  FROM (
    FROM problemlog
    SELECT *
    WHERE dt >= '${start_dt}' AND dt < '${end_dt}'
    DISTRIBUTE BY user
    SORT BY user, get_json_object(problemlog.json, '$.time_done')
  ) plog
  SELECT TRANSFORM(plog.*)
  USING 'python bayes_net_dataset.py -m ${min_ex}'
  AS topic, json
) dataset;

