-- Hive script that computes video to video correlations.
--
-- 4 parameters should be supplied
--   suffix: table postfix for the output summary tables
--   start_dt: start date stamp YYYY-mm-dd
--   end_dt: exclusive end date stamp YYYY-mm-dd
--   branch: code branch to run the recommend reducer: 'dev' or 'prod'

-- Getting (user, video, time_completed) tuples
DROP TABLE user_vid_completion_${suffix};
CREATE EXTERNAL TABLE user_vid_completion_${suffix}(
  user STRING, vid_key STRING, vid_title STRING, completion_time DOUBLE)
LOCATION 's3://ka-mapreduce/tmp/user_vid_completion_${suffix}';

INSERT OVERWRITE TABLE user_vid_completion_${suffix}
SELECT get_json_object(VideoLog.json, '$.user'),
       get_json_object(VideoLog.json, '$.video'),
       get_json_object(VideoLog.json, '$.video_title'),
       MIN(get_json_object(VideoLog.json, '$.time_watched'))
FROM VideoLog
WHERE get_json_object(VideoLog.json, '$.is_video_completed') = 'true' AND
  dt >= '${start_dt}' AND dt < '${end_dt}'
GROUP BY get_json_object(VideoLog.json, '$.user'),
         get_json_object(VideoLog.json, '$.video'),
         get_json_object(VideoLog.json, '$.video_title');

-- Getting frequency counts from the user_vid_completion table
DROP TABLE video_completion_cnt_${suffix};
CREATE EXTERNAL TABLE video_completion_cnt_${suffix}(
  vid_key STRING, cnt INT)
LOCATION 's3://ka-mapreduce/tmp/video_completion_cnt_${suffix}';

INSERT OVERWRITE TABLE video_completion_cnt_${suffix}
SELECT vid_key, COUNT(1) FROM user_vid_completion_${suffix}
GROUP BY vid_key;

-- Generating the co-ocurrence matrix
DROP TABLE video_cooccurrence_${suffix};
CREATE EXTERNAL TABLE video_coocurrence_${suffix}(
  vid1_key STRING, vid2_key STRING,
  preceed_cnt INT, succeed_cnt INT)
LOCATION 's3://ka-mapreduce/tmp/video_cooccurrence_${suffix}';

ADD FILE s3://ka-mapreduce/code/${branch}/py/video_recommendation_reducer.py;
FROM (
  FROM (
    FROM user_vid_completion_${suffix}
    SELECT user, vid_key, completion_time
    CLUSTER BY user) map_out
  SELECT TRANSFORM(map_out.*)
  USING 'video_recommendation_reducer.py'
  AS vid1_key, vid2_key, preceed_cnt, succeed_cnt) red_out
INSERT OVERWRITE TABLE video_coocurrence_${suffix}
SELECT red_out.vid1_key, red_out.vid2_key,
  SUM(red_out.preceed_cnt), SUM(red_out.succeed_cnt)
GROUP BY red_out.vid1_key, red_out.vid2_key;


-- Join the occurences matrix and the video count table together

DROP TABLE video_cooccurrence_cnt_${suffix};
CREATE EXTERNAL TABLE video_cooccurrence_cnt_${suffix}(
  vid1_key STRING, vid2_key STRING,
  preceed_cnt INT, succeed_cnt INT,
  vid1_cnt INT, vid2_cnt INT)
LOCATION 's3://ka-mapreduce/tmp/video_cooccurrence_cnt_${suffix}';

INSERT OVERWRITE TABLE video_cooccurrence_cnt_${suffix}
SELECT
  b.vid1_key, b.vid2_key,
  b.preceed_cnt, b.succeed_cnt,
  a.cnt, c.cnt
FROM video_completion_cnt_${suffix} a
JOIN video_coocurrence_${suffix} b ON (a.vid_key = b.vid1_key)
JOIN video_completion_cnt_${suffix} c ON (c.vid_key = b.vid2_key);

-- Example queries to see the top 10 video rec for a specific video
-- SELECT *, (preceed_cnt + succeed_cnt)/sqrt(vid1_cnt)/sqrt(vid2_cnt) AS
--  similarity FROM video_cooccurrence_cnt_${suffix}
-- WHERE vid1_key = "${video}"
-- ORDER BY similarity DESC LIMIT 10;

-- TODO(benkomalo): separate this out to a different file for a dedicated
-- import job flow.

-- Prune the results for importing to production (we want to limit the best
-- 20 results for each video)
DROP TABLE video_suggestions_pruned_${suffix};
CREATE EXTERNAL TABLE video_suggestions_pruned_${suffix}(
  vid1_key STRING,
  vid2_key STRING,
  score DOUBLE)
COMMENT 'Pruned video "similarity" scores (not necessarily symmetric - each entry is a vid1->vid2 score)'
LOCATION 's3://ka-mapreduce/tmp/video_suggestions_pruned_${suffix}';

ADD FILE s3://ka-mapreduce/code/${branch}/py/video_recommendation_pruner.py;

-- Pre-filter out any video pairs that have low counts to reduce noise and
-- speed up the actual reduce step.
set hivevar:MIN_COOCCURRENCE=20;

FROM (
  FROM (
    FROM video_cooccurrence_cnt_${suffix}
    SELECT *
    WHERE (preceed_cnt + succeed_cnt) >= ${MIN_COOCCURRENCE}
    CLUSTER BY vid1_key
  ) unpruned_data
  SELECT TRANSFORM(unpruned_data.*)
  USING 'video_recommendation_pruner.py'
  AS vid1_key, vid2_key, score
) pruned_data
INSERT OVERWRITE TABLE video_suggestions_pruned_${suffix}
SELECT pruned_data.*;


