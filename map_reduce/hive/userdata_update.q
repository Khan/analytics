-- Takes a partition of full UserData (in a UserDataP table), merge it with
-- incremental updates in a UserDataIncr table, and create a new UserDataP.
-- Arguments:
--     start_dt: start_date exclusive of UserDataIncr (note that the date
--         is exclusive since these partition dates indicate the inclusive
--         date up to which the data is valid for)
--     end_dt: end_dt inclusive of UserDataIncr


-- TODO(benkomalo): rename this and associated scripts, since this now does
-- it for GAEBingoIdentityRecord, in addition to UserData (since they're
-- downloaded incrementally in the exact same way)

SET mapred.reduce.tasks=128;
SET mapred.output.compress=true;
SET hive.exec.compress.output=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
SET io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;


ADD FILE s3://ka-mapreduce/code/py/find_latest_record.py;

FROM (
  FROM (
    SELECT key, json FROM UserDataP
    WHERE dt = '${start_dt}'
    UNION ALL
    SELECT key, json FROM UserDataIncr
    WHERE dt > '${start_dt}' AND dt <= '${end_dt}'
  ) map_out
  SELECT key, json CLUSTER BY key
)  red_out
INSERT OVERWRITE TABLE UserDataP PARTITION(dt='${end_dt}')
SELECT TRANSFORM(json) USING 'find_latest_record.py'
AS key, json;


-- Generating coach_summary
ADD FILE s3://ka-mapreduce/code/hive/coach_summary.q;
SOURCE /mnt/var/lib/hive_0110/downloaded_resources/coach_summary.q;


-- Creating the userdata_info_p table from UserDataP to include only user_id
-- and other core identity values related mappings so that JOINs
-- will be a lot faster.
INSERT OVERWRITE TABLE userdata_info_p PARTITION(dt='${end_dt}')
SELECT parsed.user,
  parsed.user_id,
  parsed.user_email,
  parsed.user_nickname,
  parsed.bingo_id,
  parsed.joined,
  IF(parsed.user_id RLIKE 'nouserid', 0, 1) AS registered,
  (uc.num_coaches IS NOT NULL AND uc.max_coach_students >=1) AS is_coached,
  (uc.num_coaches IS NOT NULL AND uc.max_coach_students >=10) AS is_student,
  parsed.user_data_key,
  parsed.auth_emails
FROM (
  SELECT
    get_json_object(UserDataP.json, '$.user') AS user,
    get_json_object(UserDataP.json, '$.user_id') AS user_id,
    get_json_object(UserDataP.json, '$.user_email') AS user_email,
    get_json_object(UserDataP.json, '$.user_nickname') AS user_nickname,
    get_json_object(UserDataP.json, '$.gae_bingo_identity') AS bingo_id,
    get_json_object(UserDataP.json, '$.joined') AS joined,
    -- This is the db.Key string, like ag5z...QWZDW822WwZkkDA
    get_json_object(UserDataP.json, '$.key') AS user_data_key,
    -- This returns a string that looks something like this:
    -- '["norm:mattfaus@khanacademy.org", "raw:MattFaus@KhanAcademy.ORG"]'
    -- TODO(mattfaus): Explode this to produce a row for each normal auth_email?
    get_json_object(UserDataP.json, '$.auth_emails') AS auth_emails
  FROM UserDataP WHERE dt = '${end_dt}'
) parsed LEFT OUTER JOIN
user_coach_summary uc on (parsed.user = uc.user);


FROM (
  FROM (
    SELECT key, json FROM GAEBingoIdentityRecordP
    WHERE dt = '${start_dt}'
    UNION ALL
    SELECT key, json FROM GAEBingoIdentityRecordIncr
    WHERE dt > '${start_dt}' AND dt <= '${end_dt}'
  ) map_out
  SELECT key, json CLUSTER BY key
)  red_out
INSERT OVERWRITE TABLE GAEBingoIdentityRecordP PARTITION(dt='${end_dt}')
SELECT TRANSFORM(json) USING 'find_latest_record.py --key identity'
AS key, json;

FROM (
  FROM (
    SELECT key, json FROM ScratchpadP
    WHERE dt = '${start_dt}'
    UNION ALL
    SELECT key, json FROM ScratchpadIncr
    WHERE dt > '${start_dt}' AND dt <= '${end_dt}'
  ) map_out
  SELECT key, json CLUSTER BY key
)  red_out
INSERT OVERWRITE TABLE ScratchpadP PARTITION(dt='${end_dt}')
SELECT TRANSFORM(json) USING 'find_latest_record.py'
AS key, json;

FROM (
  FROM (
    SELECT key, json FROM FeedbackP
    WHERE dt = '${start_dt}'
    UNION ALL
    SELECT key, json FROM FeedbackIncr
    WHERE dt > '${start_dt}' AND dt <= '${end_dt}'
  ) map_out
  SELECT key, json CLUSTER BY key
)  red_out
INSERT OVERWRITE TABLE FeedbackP PARTITION(dt='${end_dt}')
SELECT TRANSFORM(json) USING 'find_latest_record.py'
AS key, json;

FROM (
  FROM (
    SELECT key, json FROM UserAssessmentP
    WHERE dt = '${start_dt}'
    UNION ALL
    SELECT key, json FROM UserAssessmentIncr
    WHERE dt > '${start_dt}' AND dt <= '${end_dt}'
  ) map_out
  SELECT key, json CLUSTER BY key
)  red_out
INSERT OVERWRITE TABLE UserAssessmentP PARTITION(dt='${end_dt}')
SELECT TRANSFORM(json) USING 'find_latest_record.py'
AS key, json;

FROM (
  FROM (
    SELECT key, json FROM LearningTaskP
    WHERE dt = '${start_dt}'
    UNION ALL
    SELECT key, json FROM LearningTaskIncr
    WHERE dt > '${start_dt}' AND dt <= '${end_dt}'
  ) map_out
  SELECT key, json CLUSTER BY key
)  red_out
INSERT OVERWRITE TABLE LearningTaskP PARTITION(dt='${end_dt}')
SELECT TRANSFORM(json) USING 'find_latest_record.py'
AS key, json;

FROM (
  FROM (
    SELECT key, json FROM UserMissionP
    WHERE dt = '${start_dt}'
    UNION ALL
    SELECT key, json FROM UserMissionIncr
    WHERE dt > '${start_dt}' AND dt <= '${end_dt}'
  ) map_out
  SELECT key, json CLUSTER BY key
)  red_out
INSERT OVERWRITE TABLE UserMissionP PARTITION(dt='${end_dt}')
SELECT TRANSFORM(json) USING 'find_latest_record.py'
AS key, json;

ADD FILE s3://ka-mapreduce/code/shell/set_userdata_partition.sh;
!/mnt/var/lib/hive_0110/downloaded_resources/set_userdata_partition.sh;
