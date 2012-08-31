-- Summarizes a user's participation inside of an A/B experiment.
--
-- Inputs:
--   EXPERIMENT - the canonical name for the experiment we want to
--       summarize user participations in.

CREATE EXTERNAL TABLE IF NOT EXISTS user_experiment_info(
  user STRING,
  user_id STRING,
  user_email STRING
  )
COMMENT 'Info about a user\'s participation in an experiment'
PARTITIONED BY (experiment STRING, alternative STRING)
LOCATION 's3://ka-mapreduce/summary_tables/user_experiment_info';
ALTER TABLE user_experiment_info RECOVER PARTITIONS;


ADD FILE s3://ka-mapreduce/code/py/bingo_alternative_selector.py;

-- TODO(benkomalo): limit the query to only work on users where
--     backup_timestamp > experiment.dt_start ?

SET hive.exec.dynamic.partition=true;

-- Join experiment table and user tables
INSERT OVERWRITE TABLE user_experiment_info
PARTITION (experiment="${EXPERIMENT}", alternative)
SELECT user, user_id, user_email, alternative FROM (
  FROM (
    SELECT
        get_json_object(ud.json, '$.user') AS user,
        get_json_object(ud.json, '$.user_id') AS user_id,
        get_json_object(ud.json, '$.user_email') AS user_email,
        get_json_object(ud.json, '$.gae_bingo_identity') AS bingo_identity,
        get_json_object(bir.json, '$.pickled.participating_tests')
            AS participating_tests,
        alt.canonical_name AS canonical_name,
        alt.hashable_name AS hashable_name,
        alt.name AS alternative_name,
        alt.weight AS alternative_weight
    FROM UserData ud
    INNER JOIN GAEBingoIdentityRecord bir
      -- TODO(benkomalo): for some reason, GAEBingoIdentityRecord.key is not
      -- being correctly set - peek into the JSON for now.
      ON get_json_object(ud.json, '$.gae_bingo_identity') = get_json_object(bir.json, '$.identity')
    INNER JOIN bingo_alternative_info alt
      ON True   -- Simulate a CROSS JOIN (only available on Hive v0.10+)
    WHERE canonical_name = "${EXPERIMENT}"
    CLUSTER BY user
  ) map_output
  SELECT TRANSFORM(map_output.*)
  USING 'bingo_alternative_selector.py'
  AS user, user_id, user_email, experiment, alternative
) reduce_out;
