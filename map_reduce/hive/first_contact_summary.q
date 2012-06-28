set hivevar:start_dt=2012-04-01;
set hivevar:end_dt=2012-05-01;

DROP TABLE transient_users;
CREATE EXTERNAL TABLE transient_users(
  registered BOOLEAN,
  started_as_phantom BOOLEAN,
  is_new BOOLEAN,
  user_id STRING,
  user STRING)
PARTITIONED BY (start_dt STRING, end_dt STRING)
LOCATION 's3://ka-mapreduce/tmp/benkomalo/transient_users';
ALTER TABLE transient_users RECOVER PARTITIONS;

ALTER TABLE transient_users
    DROP PARTITION (start_dt='${start_dt}', end_dt='${end_dt}');

INSERT OVERWRITE TABLE transient_users
PARTITION (start_dt='${start_dt}', end_dt='${end_dt}')
SELECT
  userdata_info.registered,
  IF(userdata_info.user RLIKE 'nouserid', FALSE, TRUE) AS started_as_phantom,
  IF(month(from_unixtime(floor(userdata_info.joined))) = 4, TRUE, FALSE) AS is_new,
  userdata_info.user_id,
  userdata_info.user
FROM user_video_range_summary
INNER JOIN userdata_info ON
  (user_video_range_summary.user = userdata_info.user)
WHERE visits = 1 AND
  start_dt = '${start_dt}' AND
  end_dt = '${end_dt}'
ORDER BY userdata_info.registered, started_as_phantom, is_new;

CREATE EXTERNAL TABLE videos_by_transient_users(
    video_key STRING,
    video_title STRING,
    num_users_total INT,
    num_users_transient INT)
PARTITIONED BY (start_dt STRING, end_dt STRING)
LOCATION 's3://ka-mapreduce/tmp/benkomalo/videos_by_transient_users';

INSERT INTO TABLE videos_by_transient_users
PARTITION (start_dt='2012-04-01', end_dt='2012-05-01')
SELECT
  v.video_key,
  v.video_title,
  COUNT(DISTINCT v.user) as num_users_total,
  COUNT(DISTINCT u.user) as num_users_transient
FROM user_video_summary v
LEFT OUTER JOIN transient_users u
ON v.user = u.user
WHERE v.dt >= '2012-04-01' AND v.dt < '2012-05-01'
GROUP BY v.video_key, v.video_title
ORDER BY num_users_total DESC;

