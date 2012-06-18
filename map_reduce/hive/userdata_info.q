-- Creating the userdata_info table from UserData to include only user_id
-- and other core identity values related mappings so that JOINs
-- will be a lot faster.

DROP TABLE IF EXISTS userdata_info;
CREATE EXTERNAL TABLE IF NOT EXISTS userdata_info(
  user STRING,
  user_id STRING,
  user_email STRING,
  user_nickname STRING,
  joined DOUBLE,
  registered BOOLEAN
  )
LOCATION 's3://ka-mapreduce/summary_tables/userdata_info';

INSERT OVERWRITE TABLE userdata_info
SELECT parsed.*, IF(user_id RLIKE 'nouserid', 0, 1) AS registered
FROM UserData LATERAL VIEW JSON_TUPLE(UserData.json,
  'user', 'user_id', 'user_email', 'user_nickname', 'joined') parsed
AS user, user_id, user_email, user_nickname, joined;

