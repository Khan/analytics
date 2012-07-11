-- Hive script for summarizing the Feedback by user, video_key
-- 1 parameter needs to be supplied
-- dt: datestamp to summarize this thing

DROP TABLE user_feedback_summary;
CREATE EXTERNAL TABLE user_feedback_summary(
  user STRING, video_key STRING, record_cnt INT,
  question_cnt INT, answer_cnt INT) PARTITIONED BY (dt STRING)
LOCATION 's3://ka-mapreduce/summary_tables/user_feedback_summary';
ALTER TABLE user_feedback_summary RECOVER PARTITIONS;


INSERT OVERWRITE TABLE user_feedback_summary PARTITION (dt='${dt}')
SELECT
  parsed.user, parsed.vid_key, COUNT(1), SUM(parsed.q), SUM(parsed.a)
FROM (
  SELECT
    get_json_object(Feedback.json, '$.author') AS user,
    get_json_object(Feedback.json, '$.targets[0]') AS vid_key,
    IF(get_json_object(Feedback.json, '$.types[0]') = "question", 1, 0) AS q,
    IF(get_json_object(Feedback.json, '$.types[0]') = "answer", 1, 0) AS a 
  FROM Feedback
  WHERE Feedback.dt = '${dt}'
) parsed
GROUP BY parsed.user, parsed.vid_key;
