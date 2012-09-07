-- Script to generate video stats for varying duration, user category and 
-- levels(video, topic or total).
-- 3 parameters need to be supplied
-- duration: day/week/month
-- start_dt: beginning date inclusive
-- end_dt: end date stamp YYYY-mm-dd exclusive unless start_dt = end_dt

-- All users

INSERT OVERWRITE TABLE video_stats
  PARTITION (duration = '${duration}', dt = '${start_dt}', 
            user_category = 'all', aggregation = 'video')
SELECT a.title, a.youtube_id,
  COUNT(DISTINCT b.user), 
  COUNT(DISTINCT concat(b.user, b.dt)),
  SUM(IF(b.completed = true, 1, 0)),
  SUM(b.num_seconds) 
FROM (
  SELECT key, 
    get_json_object(Video.json, '$.title') AS title,
    get_json_object(Video.json, '$.youtube_id') AS youtube_id
    FROM Video
) a JOIN (
  SELECT user, dt, video_key, num_seconds, completed FROM
  user_video_summary 
  WHERE dt = '${start_dt}' or (dt > '${start_dt}' and dt < '${end_dt}')
) b ON (a.key = b.video_key)
GROUP BY a.title, a.youtube_id;


INSERT OVERWRITE TABLE video_stats
  PARTITION (duration = '${duration}', dt = '${start_dt}', 
            user_category = 'all', aggregation = 'top_topic')
SELECT a.top_category, "N/M",
  COUNT(DISTINCT b.user), 
  COUNT(DISTINCT concat(b.user, b.dt)),
  SUM(IF(b.completed = true, 1, 0)),
  SUM(b.num_seconds) 
FROM  (
  SELECT DISTINCT vid_key, top_category
  FROM video_topic_category
) a JOIN (
  SELECT user, dt, video_key, num_seconds, completed FROM
  user_video_summary 
  WHERE dt = '${start_dt}' or (dt > '${start_dt}' and dt < '${end_dt}')
) b ON (a.vid_key = b.video_key)
GROUP BY a.top_category;


INSERT OVERWRITE TABLE video_stats
  PARTITION (duration = '${duration}', dt = '${start_dt}', 
            user_category = 'all', aggregation = 'second_topic')
SELECT a.category, "N/M",
  COUNT(DISTINCT b.user), 
  COUNT(DISTINCT concat(b.user, b.dt)),
  SUM(IF(b.completed = true, 1, 0)),
  SUM(b.num_seconds) 
FROM  (
  SELECT DISTINCT vid_key,
  IF(second_category IS NULL, 'NONE', second_category) as category
  FROM video_topic_category
) a JOIN (
  SELECT user, dt, video_key, num_seconds, completed FROM
  user_video_summary 
  WHERE dt = '${start_dt}' or (dt > '${start_dt}' and dt < '${end_dt}')
) b ON (a.vid_key = b.video_key)
GROUP BY a.category;


INSERT OVERWRITE TABLE video_stats
  PARTITION (duration = '${duration}', dt = '${start_dt}', 
             user_category = 'all', aggregation = 'total')
SELECT 'Total', 'Total',
  COUNT(1),
  SUM(visits), 
  SUM(a.completed),
  SUM(a.num_seconds) 
FROM (
  SELECT user, 
    COUNT(DISTINCT dt) as visits,
    MAX(IF(completed = true, 1, 0)) as completed, 
    SUM(num_seconds) as num_seconds 
  FROM user_video_summary 
  WHERE dt = '${start_dt}' or (dt > '${start_dt}' and dt < '${end_dt}')
  GROUP BY user
) a;
   
-- Registered users
INSERT OVERWRITE TABLE video_stats
  PARTITION (duration = '${duration}', dt = '${start_dt}', 
            user_category = 'registered', aggregation = 'video')
SELECT a.title, a.youtube_id,
  COUNT(DISTINCT b.user), 
  COUNT(DISTINCT concat(b.user, b.dt)),
  SUM(IF(b.completed = true, 1, 0)),
  SUM(b.num_seconds) 
FROM (
  SELECT key, 
    get_json_object(Video.json, '$.title') AS title,
    get_json_object(Video.json, '$.youtube_id') AS youtube_id
    FROM Video
) a JOIN (
  SELECT user, dt, video_key, num_seconds, completed FROM
  user_video_summary 
  WHERE dt = '${start_dt}' or (dt > '${start_dt}' and dt < '${end_dt}')
) b ON (a.key = b.video_key) JOIN (
  SELECT user
  FROM userdata_info WHERE registered = true
) c ON (b.user = c.user)
GROUP BY a.title, a.youtube_id;


INSERT OVERWRITE TABLE video_stats
  PARTITION (duration = '${duration}', dt = '${start_dt}', 
            user_category = 'registered', aggregation = 'top_topic')
SELECT a.top_category, "N/M",
  COUNT(DISTINCT b.user), 
  COUNT(DISTINCT concat(b.user, b.dt)),
  SUM(IF(b.completed = true, 1, 0)),
  SUM(b.num_seconds) 
FROM  (
  SELECT DISTINCT vid_key, top_category
  FROM video_topic_category
) a JOIN (
  SELECT user, dt, video_key, num_seconds, completed FROM
  user_video_summary 
  WHERE dt = '${start_dt}' or (dt > '${start_dt}' and dt < '${end_dt}')
) b ON (a.vid_key = b.video_key) JOIN (
  SELECT user
  FROM userdata_info WHERE registered = true
) c ON (b.user = c.user)
GROUP BY a.top_category;


INSERT OVERWRITE TABLE video_stats
  PARTITION (duration = '${duration}', dt = '${start_dt}', 
            user_category = 'registered', aggregation = 'second_topic')
SELECT a.category, "N/M",
  COUNT(DISTINCT b.user), 
  COUNT(DISTINCT concat(b.user, b.dt)),
  SUM(IF(b.completed = true, 1, 0)),
  SUM(b.num_seconds) 
FROM  (
  SELECT DISTINCT vid_key,
  IF(second_category IS NULL, 'NONE', second_category) as category
  FROM video_topic_category
) a JOIN (
  SELECT user, dt, video_key, num_seconds, completed FROM
  user_video_summary 
  WHERE dt = '${start_dt}' or (dt > '${start_dt}' and dt < '${end_dt}')
) b ON (a.vid_key = b.video_key) JOIN (
  SELECT user
  FROM userdata_info WHERE registered = true
) c ON (b.user = c.user)
GROUP BY a.category;


INSERT OVERWRITE TABLE video_stats
  PARTITION (duration = '${duration}', dt = '${start_dt}', 
             user_category = 'registered', aggregation = 'total')
SELECT 'Total', 'N/M',
  COUNT(1),
  SUM(visits), 
  SUM(a.completed),
  SUM(a.num_seconds) 
FROM (
  SELECT user, 
    COUNT(DISTINCT dt) as visits,
    MAX(IF(completed = true, 1, 0)) as completed, 
    SUM(num_seconds) as num_seconds 
  FROM user_video_summary 
  WHERE dt = '${start_dt}' or (dt > '${start_dt}' and dt < '${end_dt}')
  GROUP BY user
) a JOIN (
  SELECT user 
  FROM userdata_info WHERE registered = true
) b ON (a.user = b.user);

-- Phantom users
INSERT OVERWRITE TABLE video_stats
  PARTITION (duration = '${duration}', dt = '${start_dt}', 
            user_category = 'phantom', aggregation = 'video')
SELECT a.title, a.youtube_id,
  COUNT(DISTINCT b.user), 
  COUNT(DISTINCT concat(b.user, b.dt)),
  SUM(IF(b.completed = true, 1, 0)),
  SUM(b.num_seconds) 
FROM (
  SELECT key, 
    get_json_object(Video.json, '$.title') AS title,
    get_json_object(Video.json, '$.youtube_id') AS youtube_id
    FROM Video
) a JOIN (
  SELECT user, dt, video_key, num_seconds, completed FROM
  user_video_summary 
  WHERE dt = '${start_dt}' or (dt > '${start_dt}' and dt < '${end_dt}')
) b ON (a.key = b.video_key) JOIN (
  SELECT user
  FROM userdata_info WHERE registered = false
) c ON (b.user = c.user)
GROUP BY a.title, a.youtube_id;


INSERT OVERWRITE TABLE video_stats
  PARTITION (duration = '${duration}', dt = '${start_dt}', 
            user_category = 'phantom', aggregation = 'top_topic')
SELECT a.top_category, "N/M",
  COUNT(DISTINCT b.user), 
  COUNT(DISTINCT concat(b.user, b.dt)),
  SUM(IF(b.completed = true, 1, 0)),
  SUM(b.num_seconds) 
FROM  (
  SELECT DISTINCT vid_key, top_category
  FROM video_topic_category
) a JOIN (
  SELECT user, dt, video_key, num_seconds, completed FROM
  user_video_summary 
  WHERE dt = '${start_dt}' or (dt > '${start_dt}' and dt < '${end_dt}')
) b ON (a.vid_key = b.video_key) JOIN (
  SELECT user
  FROM userdata_info WHERE registered = false
) c ON (b.user = c.user)
GROUP BY a.top_category;


INSERT OVERWRITE TABLE video_stats
  PARTITION (duration = '${duration}', dt = '${start_dt}', 
            user_category = 'phantom', aggregation = 'second_topic')
SELECT a.category, "N/M",
  COUNT(DISTINCT b.user), 
  COUNT(DISTINCT concat(b.user, b.dt)),
  SUM(IF(b.completed = true, 1, 0)),
  SUM(b.num_seconds) 
FROM  (
  SELECT DISTINCT vid_key,
  IF(second_category IS NULL, 'NONE', second_category) as category
  FROM video_topic_category
) a JOIN (
  SELECT user, dt, video_key, num_seconds, completed FROM
  user_video_summary 
  WHERE dt = '${start_dt}' or (dt > '${start_dt}' and dt < '${end_dt}')
) b ON (a.vid_key = b.video_key) JOIN (
  SELECT user
  FROM userdata_info WHERE registered = false
) c ON (b.user = c.user)
GROUP BY a.category;


INSERT OVERWRITE TABLE video_stats
  PARTITION (duration = '${duration}', dt = '${start_dt}', 
             user_category = 'phantom', aggregation = 'total')
SELECT 'Total', 'N/M',
  COUNT(1),
  SUM(visits), 
  SUM(a.completed),
  SUM(a.num_seconds) 
FROM (
  SELECT user, 
    COUNT(DISTINCT dt) as visits,
    MAX(IF(completed = true, 1, 0)) as completed, 
    SUM(num_seconds) as num_seconds 
  FROM user_video_summary 
  WHERE dt = '${start_dt}' or (dt > '${start_dt}' and dt < '${end_dt}')
  GROUP BY user
) a JOIN (
  SELECT user 
  FROM userdata_info WHERE registered = false
) b ON (a.user = b.user);
