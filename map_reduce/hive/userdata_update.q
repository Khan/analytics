-- Taking a partition of full UserData, merge it with incremental updates,
-- and create a new partition
-- start_dt: start_date. exclusing of UserDataIncr
-- end_dt: end_dt

SET mapred.reduce.tasks=128;
SET mapred.output.compress=true;
SET hive.exec.compress.output=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
SET io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;


ADD FILE s3://ka-mapreduce/code/dev/py/update_userdata.py; 
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
SELECT TRANSFORM(json) USING 'update_userdata.py' 
AS key, json;
