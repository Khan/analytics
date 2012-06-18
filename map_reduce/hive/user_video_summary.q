-- Hive script for summarizing the VideoLog daily by user, video
-- 1 parameter needs to be supplied
-- dt: datestamp to summarize this thing

CREATE EXTERNAL TABLE IF NOT EXISTS user_video_summary(
  user STRING, video_key STRING, video_title STRING,
  num_seconds INT, completed INT) PARTITIONED BY (dt STRING)
LOCATION 's3://ka-mapreduce/summary_tables/user_video_summary';

INSERT OVERWRITE TABLE user_video_summary PARTITION (dt='${dt}')
SELECT get_json_object(videolog.json, '$.user'),
  get_json_object(videolog.json, '$.video'),
  get_json_object(videolog.json, '$.video_title'),
  SUM(MAX(0, MIN(600, get_json_object(videolog.json, '$.seconds_watched')))),
  MAX(IF(get_json_object(videolog.json, '$.is_video_completed') = "true",
         1, 0))
FROM videolog
WHERE videolog.dt = '${dt}'
GROUP BY get_json_object(videolog.json, '$.user'),
         get_json_object(videolog.json, '$.video'),
         get_json_object(videolog.json, '$.video_title');
