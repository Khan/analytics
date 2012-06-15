-- Video Recommendation 

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
  dt >= '${start_dt}' AND dt <= '${end_dt}'
GROUP BY get_json_object(VideoLog.json, '$.user'),
         get_json_object(VideoLog.json, '$.video'),
         get_json_object(VideoLog.json, '$.video_title');

-- Getting frequency counts from the user_vid_completion table
DROP TABLE video_completion_cnt_${suffix};
CREATE EXTERNAL TABLE video_completion_cnt_${suffix}(
  vid_title STRING, cnt INT)
LOCATION 's3://ka-mapreduce/tmp/video_completion_cnt_${suffix}';

INSERT OVERWRITE TABLE video_completion_cnt_${suffix}
SELECT vid_title, COUNT(1) FROM user_vid_completion_${suffix}
GROUP BY vid_title;

-- Generating the co-ocurrence matrix
DROP TABLE video_cooccurrence_${suffix}; 
CREATE EXTERNAL TABLE video_coocurrence_${suffix}(
  vid1_title STRING, vid2_title STRING, 
  preceed_cnt INT, succeed_cnt INT)
LOCATION 's3://ka-mapreduce/tmp/video_cooccurrence_${suffix}';

ADD FILE s3://ka-mapreduce/code/${branch}/py/video_recommendation_reducer.py;
FROM(
  FROM( 
    FROM user_vid_completion_${suffix} 
    SELECT user, vid_title, completion_time 
    CLUSTER BY USER) map_out 
  SELECT TRANSFORM(map_out.*)
  USING 'video_recommendation_reducer.py' 
  AS vid1_title, vid2_title, preceed_cnt, succeed_cnt) red_out 
INSERT OVERWRITE TABLE video_coocurrence_${suffix}
SELECT red_out.vid1_title, red_out.vid2_title, 
  SUM(red_out.preceed_cnt), SUM(red_out.succeed_cnt)
GROUP BY red_out.vid1_title, red_out.vid2_title;


-- Join the occurences matrix and the video count table together 

DROP TABLE video_cooccurrence_cnt_${suffix};
CREATE EXTERNAL TABLE video_cooccurrence_cnt_${suffix}(
  vid1_title STRING, vid2_title STRING, 
  preceed_cnt INT, succeed_cnt INT, 
  vid1_cnt INT, vid2_cnt INT)
LOCATION 's3://ka-mapreduce/tmp/video_cooccurrence_cnt_${suffix}'; 

INSERT OVERWRITE TABLE video_cooccurrence_cnt_${suffix} 
SELECT b.vid1_title, b.vid2_title, b.preceed_cnt, b.succeed_cnt,
       a.cnt, c.cnt 
FROM video_completion_cnt_${suffix} a JOIN 
  video_coocurrence_${suffix} b ON (a.vid_title = b.vid1_title) JOIN
  video_completion_cnt_${suffix} c on (c.vid_title = b.vid2_title);

-- Example queries to see the top 
-- SELECT *, (preceed_cnt + succeed_cnt)/sqrt(vid1_cnt)/sqrt(vid2_cnt) AS
--  similarity FROM video_cooccurrence_cnt_${suffix} 
-- WHERE vid1_title = "American Call Options"
-- ORDER BY similarity DESC LIMIT 10;
