-- Summarizes a user's participation inside of an A/B experiment.
--
-- Inputs:
--   EXPERIMENT - the canonical name for the experiment we want to
--       summarize user participations in.
--   EXP_PARTITION - used for the experiment partition name beacuse
--       because S3 does not like underscores etc in bucket names
--   dt - This effectively specifies as as-of date for the analysis. Because
--        GAEBingo prunes the data in GAEBingoIdentityRecord after experiments
--        are archived, if the experiment you are interested in is no longer
--        running you should set this to the last day it was still live.
-- Example Inputs:
-- set hivevar:EXPERIMENT=Accuracy model: Early proficiency;
-- set hivevar:EXP_PARTITION=early-prof;
-- set hivevar:dt=2012-12-05;

ADD FILE s3://ka-mapreduce/code/py/bingo_alternative_selector.py;

CREATE EXTERNAL TABLE IF NOT EXISTS user_experiment_info(
  user STRING,
  user_id STRING,
  user_email STRING,
  bingo_identity STRING
  )
COMMENT 'Info about a user\'s participation in an experiment'
PARTITIONED BY (experiment STRING, alternative STRING)
LOCATION 's3://ka-mapreduce/summary_tables/user_experiment_info';
ALTER TABLE user_experiment_info RECOVER PARTITIONS;


SET hive.exec.dynamic.partition=true;

INSERT OVERWRITE TABLE user_experiment_info
PARTITION (experiment="${EXP_PARTITION}", alternative)
SELECT 
  get_json_object(ud.json, '$.user') AS user,
  get_json_object(ud.json, '$.user_id') AS user_id,
  get_json_object(ud.json, '$.user_email') AS user_email,
  id_alt.identity, 
  id_alt.alternative
FROM
(
  -- Create a map from bingo identities to alternatives for this experiment
  FROM (
    SELECT
        get_json_object(bir.json, '$.identity') AS bingo_identity,
        get_json_object(bir.json, '$.pickled.participating_tests')
            AS participating_tests,
        alt.canonical_name AS canonical_name,
        alt.hashable_name AS hashable_name,
        alt.name AS alternative_name,
        alt.weight AS alternative_weight,
        alt.number AS alternative_number
    FROM bingo_alternative_infop alt
    INNER JOIN GAEBingoIdentityRecord bir
      ON True   -- Simulate a CROSS JOIN (only available on Hive v0.10+)
    WHERE alt.canonical_name = "${EXPERIMENT}" AND alt.dt = "${dt}"
    CLUSTER BY bingo_identity
  ) map_output
  SELECT TRANSFORM(map_output.*)
  USING 'bingo_alternative_selector.py'
  AS identity, experiment, alternative

) id_alt
-- now annotate the bingo id -> alternative map with UserData info
-- TODO(jace): we should include the bingo identity in userdata_info
INNER JOIN UserData ud
ON id_alt.identity = get_json_object(ud.json, '$.gae_bingo_identity')
;
