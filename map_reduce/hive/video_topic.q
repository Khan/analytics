-- Generating Video to Topic mapping. The relationship is one to many.

ADD FILE s3://ka-mapreduce/code/py/ka_udf.py;

CREATE EXTERNAL TABLE IF NOT EXISTS video_topic(
  vid_key STRING, vid_title STRING, topic_key STRING,
  topic_title STRING, topic_desc STRING)
LOCATION 's3://ka-mapreduce/summary_tables/video_topic';

FROM(
  FROM Video SELECT TRANSFORM(Video.json)
  USING 'ka_udf.py split topic_string_keys "<tab>" key,title 0' as
  topic_key, vid_key, vid_title
) exploded_video JOIN Topic ON
  (exploded_video.topic_key = get_json_object(Topic.json, '$.key'))
INSERT OVERWRITE TABLE video_topic
SELECT exploded_video.vid_key, exploded_video.vid_title,
  get_json_object(Topic.json, '$.key'),
  get_json_object(Topic.json, '$.title'),
  get_json_object(Topic.json, '$.description');
