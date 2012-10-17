-- Hive script for summarizing the Feedback by user, video_key
-- 1 parameter needs to be supplied
-- dt: datestamp to summarize this thing

-- TODO(benkomalo): this is a little stale, since vid_key is no longer
-- guaranteed to be in targets[0] - there are discussions on CS scratchpads
-- and potentially other things in the future.
-- TODO(benkomalo): there are other types other than question and answer
-- (e.g. comments) - account for that in the schema below.


INSERT OVERWRITE TABLE user_feedback_summary PARTITION (dt='${dt}')
SELECT
  parsed.user, parsed.vid_key, COUNT(1), SUM(parsed.q), SUM(parsed.a)
FROM (
  SELECT
    get_json_object(FeedbackIncr.json, '$.author') AS user,
    get_json_object(FeedbackIncr.json, '$.targets[0]') AS vid_key,
    IF(get_json_object(FeedbackIncr.json, '$.types[0]') = "question", 1, 0) AS q,
    IF(get_json_object(FeedbackIncr.json, '$.types[0]') = "answer", 1, 0) AS a
  FROM FeedbackIncr
  WHERE FeedbackIncr.dt = '${dt}'
) parsed
GROUP BY parsed.user, parsed.vid_key;
