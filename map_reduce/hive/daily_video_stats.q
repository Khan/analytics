INSERT OVERWRITE TABLE daily_video_stats
  PARTITION (dt = '${dt}', user_category = 'all', aggregation = 'video')
SELECT a.title, a.youtube_id,
  COUNT(1), 
  SUM(IF(b.completed = true, 1, 0)),
  SUM(b.num_seconds) 
FROM (
  SELECT key, 
    get_json_object(Video.json, '$.title') AS title,
    get_json_object(Video.json, '$.youtube_id') AS youtube_id
    FROM Video
) a JOIN (
  SELECT video_key, num_seconds, completed FROM
  user_video_summary WHERE dt = '${dt}'
) b ON (a.key = b.video_key)
GROUP BY a.title, a.youtube_id;

INSERT OVERWRITE TABLE daily_video_stats
  PARTITION (dt = '${dt}', user_category = 'all', aggregation = 'total')
SELECT 'Total', 'Total',
  COUNT(1),
  SUM(a.completed),
  SUM(a.num_seconds) 
FROM (
  SELECT user, 
    MAX(IF(completed = true, 1, 0)) as completed, 
    SUM(num_seconds) as num_seconds 
  FROM user_video_summary WHERE dt = '${dt}'
  GROUP BY user
) a;
   

INSERT OVERWRITE TABLE daily_video_stats
  PARTITION (dt = '${dt}', user_category = 'registered', aggregation = 'video')
SELECT a.title, a.youtube_id,
  COUNT(1), 
  SUM(IF(b.completed = true, 1, 0)),
  SUM(b.num_seconds) 
FROM (
  SELECT key, 
    get_json_object(Video.json, '$.title') AS title,
    get_json_object(Video.json, '$.youtube_id') AS youtube_id
    FROM Video
) a JOIN (
  SELECT user, video_key, num_seconds, completed FROM
  user_video_summary WHERE dt = '${dt}'
) b ON (a.key = b.video_key) JOIN (
  SELECT user
  FROM userdata_info WHERE registered = true
) c ON (b.user = c.user)
GROUP BY a.title, a.youtube_id;

INSERT OVERWRITE TABLE daily_video_stats
  PARTITION (dt = '${dt}', user_category = 'registered', aggregation = 'total')
SELECT 'Total', 'Total',
  COUNT(1),
  SUM(a.completed),
  SUM(a.num_seconds) 
FROM (
  SELECT user, 
    MAX(IF(completed = true, 1, 0)) as completed, 
    SUM(num_seconds) as num_seconds 
  FROM user_video_summary WHERE dt = '${dt}'
  GROUP BY user
) a JOIN (
  SELECT user 
  FROM userdata_info WHERE registered = true
) b ON (a.user = b.user);

INSERT OVERWRITE TABLE daily_video_stats
  PARTITION (dt = '${dt}', user_category = 'phantom', aggregation = 'video')
SELECT a.title, a.youtube_id,
  COUNT(1), 
  SUM(IF(b.completed = true, 1, 0)),
  SUM(b.num_seconds) 
FROM (
  SELECT key, 
    get_json_object(Video.json, '$.title') AS title,
    get_json_object(Video.json, '$.youtube_id') AS youtube_id
    FROM Video
) a JOIN (
  SELECT user, video_key, num_seconds, completed FROM
  user_video_summary WHERE dt = '${dt}'
) b ON (a.key = b.video_key) JOIN (
  SELECT user
  FROM userdata_info WHERE registered = false
) c ON (b.user = c.user)
GROUP BY a.title, a.youtube_id;

INSERT OVERWRITE TABLE daily_video_stats
  PARTITION (dt = '${dt}', user_category = 'phantom', aggregation = 'total')
SELECT 'Total', 'Total',
  COUNT(1),
  SUM(a.completed),
  SUM(a.num_seconds) 
FROM (
  SELECT user, 
    MAX(IF(completed = true, 1, 0)) as completed, 
    SUM(num_seconds) as num_seconds 
  FROM user_video_summary WHERE dt = '${dt}'
  GROUP BY user
) a JOIN (
  SELECT user 
  FROM userdata_info WHERE registered = false
) b ON (a.user = b.user);
