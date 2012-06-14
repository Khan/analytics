-- Creating the userdata_ids table from UserData to include only user_id 
-- related mappings so that JOINs will be a lot faster.

CREATE EXTERNAL TABLE IF NOT EXISTS userdata_ids(
  user STRING, user_id STRING, user_email STRING, 
  current_user STRING, user_nickname STRING, joined DOUBLE) 
LOCATION 's3://ka-mapreduce/summary_tables/userdata_ids';

INSERT OVERWRITE TABLE userdata_ids                             
SELECT get_json_object(UserData.json, '$.user'),
  get_json_object(UserData.json, '$.user_id'),
  get_json_object(UserData.json, '$.user_email'),
  get_json_object(UserData.json, '$.current_user'),
  get_json_object(UserData.json, '$.user_nickname'),
  get_json_object(UserData.json, '$.joined') 
FROM UserData;
