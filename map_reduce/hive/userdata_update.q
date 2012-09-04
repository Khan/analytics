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

ADD FILE s3://ka-mapreduce/code/shell/set_userdata_partition.sh;
!/mnt/var/lib/hive_081/downloaded_resources/set_userdata_partition.sh;
