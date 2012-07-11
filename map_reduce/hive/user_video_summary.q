-- Hive script for summarizing the VideoLog daily by user, video
-- 1 parameter needs to be supplied
-- dt: datestamp to summarize this thing

-- LATERAL VIEW doesn't work with WHERE clause, otherwise we'd
-- use the more efficient json_tuple UDTF.
-- https://issues.apache.org/jira/browse/HIVE-1056
INSERT OVERWRITE TABLE user_video_summary PARTITION (dt='${dt}')
SELECT
  parsed.user, parsed.video, parsed.video_title,
  SUM(IF(parsed.seconds_watched < 0,  -- Clamp in [0, 1800) seconds for sanity
         0,
         IF(parsed.seconds_watched > 1800,
            1800,
            parsed.seconds_watched
      ))),
  MAX(IF(parsed.completed, 1, 0)) == 1
FROM (
  SELECT
    get_json_object(videolog.json, '$.user') as user,
    get_json_object(videolog.json, '$.video') as video,
    get_json_object(videolog.json, '$.video_title') as video_title,
    get_json_object(videolog.json, '$.seconds_watched') as seconds_watched,
    get_json_object(videolog.json, '$.is_video_completed') = "true" as completed
  FROM videolog
  WHERE videolog.dt = '${dt}'
) parsed
GROUP BY parsed.user, parsed.video, parsed.video_title;
