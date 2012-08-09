-- Hive script to compute dataset for user engagement study 
-- 2 paramters
-- start_dt: start date stamp YYYY-mm-dd
-- end_dt: exclusive end date stamp YYYY-mm-dd


-- type is to describe type of visits
-- total: # visits across video, exercise, feedback
-- video_exercise: # visits across video and exercise
-- ditto for other types of visits. 
DROP TABLE user_classification;
CREATE EXTERNAL TABLE IF NOT EXISTS user_classification(
user STRING, 
visits INT) 
PARTITIONED BY (start_dt string, end_dt string, type string)
LOCATION 's3://ka-mapreduce/summary_tables/training_set/user_classification';

INSERT OVERWRITE TABLE user_classification 
  PARTITION (start_dt='${start_dt}', end_dt='${end_dt}', type = 'total')
SELECT unioned.user as user, COUNT(DISTINCT unioned.dt) as visits
FROM (
  SELECT user, dt FROM user_video_summary 
  WHERE dt >= '${start_dt}' AND dt < '${end_dt}'  
  UNION ALL 
  SELECT user, dt FROM user_exercise_summary 
  WHERE dt >= '${start_dt}' AND dt < '${end_dt}'
  UNION ALL 
  SELECT user, dt from user_feedback_summary 
  WHERE dt >= '${start_dt}' and dt < '${end_dt}' 
) unioned GROUP BY unioned.user;


INSERT OVERWRITE TABLE user_classification 
  PARTITION (start_dt='${start_dt}', end_dt='${end_dt}', 
             type = 'video_exercise')
SELECT unioned.user as user, COUNT(DISTINCT unioned.dt) as visits
FROM (
  SELECT user, dt FROM user_video_summary 
  WHERE dt >= '${start_dt}' AND dt < '${end_dt}'  
  UNION ALL 
  SELECT user, dt FROM user_exercise_summary 
  WHERE dt >= '${start_dt}' AND dt < '${end_dt}'
) unioned GROUP BY unioned.user;


INSERT OVERWRITE TABLE user_classification 
  PARTITION (start_dt='${start_dt}', end_dt='${end_dt}', 
             type = 'video_feedback')
SELECT unioned.user as user, COUNT(DISTINCT unioned.dt) as visits
FROM (
  SELECT user, dt FROM user_video_summary 
  WHERE dt >= '${start_dt}' AND dt < '${end_dt}'  
  UNION ALL 
  SELECT user, dt FROM user_feedback_summary 
  WHERE dt >= '${start_dt}' AND dt < '${end_dt}'
) unioned GROUP BY unioned.user;


INSERT OVERWRITE TABLE user_classification 
  PARTITION (start_dt='${start_dt}', end_dt='${end_dt}', 
  type = 'exercise_feedback')
SELECT unioned.user as user, COUNT(DISTINCT unioned.dt) as visits
FROM (
  SELECT user, dt FROM user_feedback_summary 
  WHERE dt >= '${start_dt}' AND dt < '${end_dt}'  
  UNION ALL 
  SELECT user, dt FROM user_exercise_summary 
  WHERE dt >= '${start_dt}' AND dt < '${end_dt}'
) unioned GROUP BY unioned.user;

INSERT OVERWRITE TABLE user_classification 
  PARTITION (start_dt='${start_dt}', end_dt='${end_dt}', type = 'video')
SELECT user, COUNT(DISTINCT dt) as visits FROM user_video_summary 
WHERE dt >= '${start_dt}' AND dt < '${end_dt}'  
GROUP BY user;

INSERT OVERWRITE TABLE user_classification 
PARTITION 
  (start_dt='${start_dt}', end_dt='${end_dt}', type = 'exercise')
SELECT user, COUNT(DISTINCT dt) as visits FROM user_exercise_summary 
WHERE dt >= '${start_dt}' AND dt < '${end_dt}'  
GROUP BY user;


INSERT OVERWRITE TABLE user_classification 
PARTITION 
  (start_dt='${start_dt}', end_dt='${end_dt}', type = 'feedback')
SELECT user, count(distinct dt) as visits FROM user_feedback_summary 
WHERE dt >= '${start_dt}' AND dt < '${end_dt}'  
GROUP BY user;


DROP TABLE user_visits_summary;
CREATE EXTERNAL TABLE IF NOT EXISTS user_visits_summary(
user STRING,
user_id STRING, 
user_email STRING,
joined_date STRING,
registered BOOLEAN,
total_visits INT,
video_visits INT,
exercise_visits INT,
feedback_visits INT,
video_exercise_visits INT,
video_feedback_visits INT,
exercise_feedback_visits INT
) PARTITIONED BY (start_dt string, end_dt string)
LOCATION 's3://ka-mapreduce/summary_tables/training_set/user_visits_summary';

INSERT OVERWRITE TABLE user_visits_summary 
PARTITION (start_dt='${start_dt}', end_dt='${end_dt}')
SELECT a.user, b.user_id, b.user_email, 
  from_unixtime(floor(b.joined), "yyyyMMdd"), 
  b.registered, a.total_visits,
  a.video_visits, a.exercise_visits, a.feedback_visits,
  a.video_exercise_visits, a.video_feedback_visits, a.exercise_feedback_visits
FROM (
  SELECT user, 
    SUM(IF(type='total', visits, 0)) as total_visits,
    SUM(IF(type='video', visits, 0)) as video_visits,
    SUM(IF(type='exercise', visits, 0)) as exercise_visits,
    SUM(IF(type='feedback', visits, 0)) as feedback_visits,
    SUM(IF(type='video_exercise', visits, 0)) as video_exercise_visits,
    SUM(IF(type='video_feedback', visits, 0)) as video_feedback_visits,
    SUM(IF(type='exercise_feedback', visits, 0)) as exercise_feedback_visits
  FROM user_classification 
  WHERE start_dt = '${start_dt}' AND end_dt = '${end_dt}'
  GROUP BY user
) a LEFT OUTER JOIN userdata_info b on (a.user = b.user);


-- a bunch of queries to summarize each  type of activity better
DROP TABLE user_exercise_condensed;
CREATE TABLE user_exercise_condensed(
  user STRING,
  time_spent INT,
  num_problems INT,
  num_proficient INT,
  accuracy DOUBLE 
) PARTITIONED by (start_dt STRING, end_dt STRING);

INSERT OVERWRITE TABLE user_exercise_condensed
PARTITION (start_dt='${start_dt}', end_dt='${end_dt}')
SELECT user, SUM(time_spent), COUNT(1), 
  SUM(IF(proficient = true, 1, 0)), SUM(num_correct)/SUM(num_correct+num_wrong)
FROM user_exercise_summary 
WHERE dt >= '${start_dt}' AND dt < '${end_dt}'
GROUP BY user;
  
DROP TABLE user_video_condensed;
CREATE TABLE user_video_condensed(
  user STRING,
  num_videos INT,
  num_completed INT,
  num_seconds INT
) PARTITIONED by (start_dt STRING, end_dt STRING);

INSERT OVERWRITE TABLE user_video_condensed
PARTITION (start_dt='${start_dt}', end_dt='${end_dt}')
SELECT user, COUNT(1), SUM(IF(completed = true, 1, 0)), SUM(num_seconds)
FROM user_video_summary
WHERE dt >= '${start_dt}' AND dt < '${end_dt}'
GROUP BY user;

DROP TABLE user_feedback_condensed;
CREATE TABLE user_feedback_condensed(
  user STRING, 
  num_entities INT, 
  num_comments INT,
  num_questions INT,
  num_answers INT
) PARTITIONED by (start_dt STRING, end_dt STRING);

INSERT OVERWRITE TABLE user_feedback_condensed
PARTITION (start_dt='${start_dt}', end_dt='${end_dt}')
SELECT user, COUNT(1), SUM(record_cnt), SUM(question_cnt), SUM(answer_cnt)
FROM user_feedback_summary
WHERE dt >= '${start_dt}' AND dt < '${end_dt}'
GROUP BY user;

-- video summary by topic
CREATE EXTERNAL TABLE IF NOT EXISTS vid_category_summary(
  topic STRING, videos INT, 
  users INT, visits INT, seconds INT, completed INT) 
PARTITIONED BY (start_dt string, end_dt string, category string)
LOCATION 
  's3://ka-mapreduce/summary_tables/training_set/vid_category_summary';

INSERT OVERWRITE TABLE vid_category_summary 
  PARTITION (start_dt='${start_dt}', end_dt='${end_dt}', category = 'top')  
SELECT b.top_category, COUNT(DISTINCT a.video_key) videos, 
  COUNT(DISTINCT a.user) users,
  COUNT(DISTINCT concat(a.user, a.dt)) visits,
  SUM(a.num_seconds) seconds, 
  SUM(IF(completed = true, 1, 0)) completed
FROM user_video_summary a JOIN (
  SELECT DISTINCT vid_key, top_category 
  FROM video_topic_category
) b ON (a.video_key = b.vid_key)
WHERE a.dt >= '${start_dt}' AND a.dt < '${end_dt}'  
GROUP BY b.top_category;

INSERT OVERWRITE TABLE vid_category_summary 
  PARTITION (start_dt='${start_dt}', end_dt='${end_dt}', category = 'second')  
SELECT b.category, COUNT(DISTINCT a.video_key) videos, 
  COUNT(DISTINCT a.user) users,
  COUNT(DISTINCT concat(a.user, a.dt)) visits,
  SUM(a.num_seconds) seconds, 
  SUM(IF(completed = true, 1, 0)) completed
FROM user_video_summary a JOIN (
  SELECT DISTINCT vid_key, 
  IF(second_category IS NULL, 'NONE', second_category) as category
  FROM video_topic_category
) b ON (a.video_key = b.vid_key)
WHERE a.dt >= '${start_dt}' AND a.dt < '${end_dt}'  
GROUP BY b.category;

-- video viewed by topic
CREATE EXTERNAL TABLE IF NOT EXISTS user_vid_category_summary(
user STRING, 
topic STRING,
videos INT, visits INT, seconds INT, completed INT) 
PARTITIONED BY (start_dt string, end_dt string, category string)
LOCATION 
  's3://ka-mapreduce/summary_tables/training_set/user_vid_category_summary';

INSERT OVERWRITE TABLE user_vid_category_summary 
  PARTITION (start_dt='${start_dt}', end_dt='${end_dt}', category = 'top')  
SELECT a.user, b.top_category, COUNT(DISTINCT a.video_key) videos, 
  COUNT(DISTINCT a.dt) visits, SUM(a.num_seconds) seconds, 
  SUM(IF(completed = true, 1, 0)) completed
FROM user_video_summary a JOIN (
  SELECT DISTINCT vid_key, top_category 
  FROM video_topic_category
) b ON (a.video_key = b.vid_key)
WHERE a.dt >= '${start_dt}' AND a.dt < '${end_dt}'  
GROUP BY a.user, b.top_category;

INSERT OVERWRITE TABLE user_vid_category_summary 
  PARTITION (start_dt='${start_dt}', end_dt='${end_dt}', category = 'second')  
SELECT a.user, b.category, COUNT(DISTINCT a.video_key) videos, 
  COUNT(DISTINCT a.dt) visits, SUM(a.num_seconds) seconds, 
  SUM(IF(completed = true, 1, 0)) completed
FROM user_video_summary a JOIN (
  SELECT DISTINCT vid_key, 
  IF(second_category IS NULL, top_category, second_category) as category
  FROM video_topic_category
) b ON (a.video_key = b.vid_key)
WHERE a.dt >= '${start_dt}' AND a.dt < '${end_dt}'  
GROUP BY a.user, b.category;


CREATE EXTERNAL TABLE IF NOT EXISTS user_video_top_category_stat(
  user STRING, 
  math_videos INT, math_visits INT, 
  math_seconds INT, math_completed INT,
  science_videos INT, science_visits INT, 
  science_seconds INT, science_completed INT,
  humanities_videos INT, humanities_visits INT, 
  humanities_seconds INT, humanities_completed INT,
  noteworthy_videos INT, noteworthy_visits INT, 
  noteworthy_seconds INT, noteworthy_completed INT,
  finance_videos INT, finance_visits INT, 
  finance_seconds INT, finance_completed INT,
  testprep_videos INT, testprep_visits INT, 
  testprep_seconds INT, testprep_completed INT,
  interview_videos INT, interview_visits INT, 
  interview_seconds INT, interview_completed INT
) PARTITIONED BY (start_dt STRING, end_dt STRING)
LOCATION 
  's3://ka-mapreduce/summary_tables/training_set/user_video_top_category_stat';

INSERT OVERWRITE TABLE user_video_top_category_stat 
PARTITION (start_dt='${start_dt}', end_dt='${end_dt}') 
SELECT user,
  SUM(IF(topic = 'Math', videos, 0)),
  SUM(IF(topic = 'Math', visits, 0)),
  SUM(IF(topic = 'Math', seconds, 0)),
  SUM(IF(topic = 'Math', completed, 0)),
  SUM(IF(topic = 'Science', videos, 0)),
  SUM(IF(topic = 'Science', visits, 0)),
  SUM(IF(topic = 'Science', seconds, 0)),
  SUM(IF(topic = 'Science', completed, 0)),
  SUM(IF(topic = 'Humanities', videos, 0)),
  SUM(IF(topic = 'Humanities', visits, 0)),
  SUM(IF(topic = 'Humanities', seconds, 0)),
  SUM(IF(topic = 'Humanities', completed, 0)),
  SUM(IF(topic = 'New and Noteworthy', videos, 0)),
  SUM(IF(topic = 'New and Noteworthy', visits, 0)),
  SUM(IF(topic = 'New and Noteworthy', seconds, 0)),
  SUM(IF(topic = 'New and Noteworthy', completed, 0)),
  SUM(IF(topic = 'Finance & Econ.', videos, 0)),
  SUM(IF(topic = 'Finance & Econ.', visits, 0)),
  SUM(IF(topic = 'Finance & Econ.', seconds, 0)),
  SUM(IF(topic = 'Finance & Econ.', completed, 0)),
  SUM(IF(topic = 'Test Prep', videos, 0)),
  SUM(IF(topic = 'Test Prep', visits, 0)),
  SUM(IF(topic = 'Test Prep', seconds, 0)),
  SUM(IF(topic = 'Test Prep', completed, 0)),
  SUM(IF(topic = 'Talks and Interviews', videos, 0)),
  SUM(IF(topic = 'Talks and Interviews', visits, 0)),
  SUM(IF(topic = 'Talks and Interviews', seconds, 0)),
  SUM(IF(topic = 'Talks and Interviews', completed, 0))
FROM user_vid_category_summary 
WHERE category = 'top' AND start_dt = '${start_dt}' AND end_dt = '${end_dt}'
GROUP by user;
 
-- an alternative table to serialize category usage into a json string. 
-- the json includes 2nd-level categories
CREATE EXTERNAL TABLE IF NOT EXISTS user_video_category_flattened(
user STRING, 
category_json STRING
) PARTITIONED BY (start_dt STRING, end_dt STRING)
LOCATION 
  's3://ka-mapreduce/summary_tables/training_set/user_video_category_flattened';

INSERT OVERWRITE TABLE user_video_category_flattened 
PARTITION (start_dt='${start_dt}', end_dt='${end_dt}')
SELECT red_out.user, red_out.category_json
FROM(
  FROM (
    SELECT user, topic, videos, visits, seconds, completed FROM
    user_vid_category_summary 
    WHERE start_dt = '${start_dt}' AND end_dt = '${end_dt}'
    CLUSTER BY user
  ) map_out 
  SELECT TRANSFORM(map_out.*) 
  USING 
  's3://ka-mapreduce/code/py/user_video_category_reducer.py -k 1 -v 2,3,4,5'
  AS user, category_json
) red_out;

-- join user_visits_summary, user_vid_category_stat, and condensed tables

DROP TABLE user_engagement_dataset;
CREATE EXTERNAL TABLE user_engagement_dataset(
  user STRING,
  user_id STRING,
  user_email STRING,
  joined_date STRING,
  total_visits INT,
  video_visits INT,
  exercise_visits INT,
  feedback_visits INT,
  video_exercise_visits INT,
  video_feedback_visits INT,
  exercise_feedback_visits INT,
  timespent_exercise INT,
  num_exercise INT,
  num_proficient INT,
  avg_accuracy INT, 
  timespent_video INT,
  num_videos INT, 
  num_completed INT, 
  num_videos_commented INT,
  num_comments INT,
  num_questions INT, 
  num_answers INT, 
  math_videos INT, math_visits INT, 
  math_seconds INT, math_completed INT,
  science_videos INT, science_visits INT, 
  science_seconds INT, science_completed INT,
  humanities_videos INT, humanities_visits INT, 
  humanities_seconds INT, humanities_completed INT,
  noteworthy_videos INT, noteworthy_visits INT, 
  noteworthy_seconds INT, noteworthy_completed INT,
  finance_videos INT, finance_visits INT, 
  finance_seconds INT, finance_completed INT,
  testprep_videos INT, testprep_visits INT, 
  testprep_seconds INT, testprep_completed INT,
  interview_videos INT, interview_visits INT, 
  interview_seconds INT, interview_completed INT
) PARTITIONED BY (start_dt STRING, end_dt STRING, type STRING)
LOCATION 's3://ka-mapreduce/summary_tables/training_set/user_engagement_dataset';

INSERT OVERWRITE TABLE user_engagement_dataset 
PARTITION (start_dt = '${start_dt}', end_dt = '${end_dt}', type = 'phantom')
SELECT a.user, a.user_id, a.user_email, a.joined_date,
  a.total_visits, a.video_visits, a.exercise_visits, a.feedback_visits,
  a.video_exercise_visits, a.video_feedback_visits, a.exercise_feedback_visits,
  b.time_spent, b.num_problems, b.num_proficient, b.accuracy, 
  c.num_seconds, c.num_videos, c.num_completed,
  d.num_entities, d.num_comments, d.num_questions, d.num_answers,
  e.math_videos, e.math_visits, 
  e.math_seconds, e.math_completed,
  e.science_videos, e.science_visits, 
  e.science_seconds, e.science_completed,
  e.humanities_videos, e.humanities_visits, 
  e.humanities_seconds, e.humanities_completed,
  e.noteworthy_videos, e.noteworthy_visits, 
  e.noteworthy_seconds, e.noteworthy_completed,
  e.finance_videos, e.finance_visits, 
  e.finance_seconds, e.finance_completed,
  e.testprep_videos, e.testprep_visits, 
  e.testprep_seconds, e.testprep_completed,
  e.interview_videos, e.interview_visits, 
  e.interview_seconds, e.interview_completed
FROM (
  SELECT user, user_id, user_email, joined_date,
    total_visits, video_visits, exercise_visits, feedback_visits,
    video_exercise_visits, video_feedback_visits, exercise_feedback_visits
  FROM user_visits_summary 
  WHERE start_dt = '${start_dt}' and end_dt = '${end_dt}'
    AND registered = false
) a LEFT OUTER JOIN (
  SELECT user, time_spent, num_problems, num_proficient, accuracy 
  FROM user_exercise_condensed 
  WHERE start_dt = '${start_dt}' and end_dt = '${end_dt}'
) b on (a.user = b.user) LEFT OUTER JOIN (
  SELECT user, num_seconds, num_videos, num_completed
  FROM user_video_condensed 
  WHERE start_dt = '${start_dt}' and end_dt = '${end_dt}'
) c on (a.user = c.user) LEFT OUTER JOIN (
  SELECT user, num_entities, num_comments, num_questions, num_answers
  FROM user_feedback_condensed 
  WHERE start_dt = '${start_dt}' and end_dt = '${end_dt}'
) d on (a.user = d.user) LEFT OUTER JOIN (
  SELECT user, 
  math_videos, math_visits, 
  math_seconds, math_completed,
  science_videos, science_visits, 
  science_seconds, science_completed,
  humanities_videos, humanities_visits, 
  humanities_seconds, humanities_completed,
  noteworthy_videos, noteworthy_visits, 
  noteworthy_seconds, noteworthy_completed,
  finance_videos, finance_visits, 
  finance_seconds, finance_completed,
  testprep_videos, testprep_visits, 
  testprep_seconds, testprep_completed,
  interview_videos, interview_visits, 
  interview_seconds, interview_completed
  FROM user_video_top_category_stat
  WHERE start_dt = '${start_dt}' and end_dt = '${end_dt}'
) e on (a.user = e.user); 

INSERT OVERWRITE TABLE user_engagement_dataset 
PARTITION (start_dt = '${start_dt}', end_dt = '${end_dt}', type = 'registered')
SELECT a.user, a.user_id, a.user_email, a.joined_date,
  a.total_visits, a.video_visits, a.exercise_visits, a.feedback_visits,
  a.video_exercise_visits, a.video_feedback_visits, a.exercise_feedback_visits,
  b.time_spent, b.num_problems, b.num_proficient, b.accuracy, 
  c.num_seconds, c.num_videos, c.num_completed,
  d.num_entities, d.num_comments, d.num_questions, d.num_answers,
  e.math_videos, e.math_visits, 
  e.math_seconds, e.math_completed,
  e.science_videos, e.science_visits, 
  e.science_seconds, e.science_completed,
  e.humanities_videos, e.humanities_visits, 
  e.humanities_seconds, e.humanities_completed,
  e.noteworthy_videos, e.noteworthy_visits, 
  e.noteworthy_seconds, e.noteworthy_completed,
  e.finance_videos, e.finance_visits, 
  e.finance_seconds, e.finance_completed,
  e.testprep_videos, e.testprep_visits, 
  e.testprep_seconds, e.testprep_completed,
  e.interview_videos, e.interview_visits, 
  e.interview_seconds, e.interview_completed
FROM (
  SELECT user, user_id, user_email, joined_date,
    total_visits, video_visits, exercise_visits, feedback_visits,
    video_exercise_visits, video_feedback_visits, exercise_feedback_visits
  FROM user_visits_summary 
  WHERE start_dt = '${start_dt}' and end_dt = '${end_dt}'
    AND registered = true
) a LEFT OUTER JOIN (
  SELECT user, time_spent, num_problems, num_proficient, accuracy 
  FROM user_exercise_condensed 
  WHERE start_dt = '${start_dt}' and end_dt = '${end_dt}'
) b on (a.user = b.user) LEFT OUTER JOIN (
  SELECT user, num_seconds, num_videos, num_completed
  FROM user_video_condensed 
  WHERE start_dt = '${start_dt}' and end_dt = '${end_dt}'
) c on (a.user = c.user) LEFT OUTER JOIN (
  SELECT user, num_entities, num_comments, num_questions, num_answers
  FROM user_feedback_condensed 
  WHERE start_dt = '${start_dt}' and end_dt = '${end_dt}'
) d on (a.user = d.user) LEFT OUTER JOIN (
  SELECT user, 
  math_videos, math_visits, 
  math_seconds, math_completed,
  science_videos, science_visits, 
  science_seconds, science_completed,
  humanities_videos, humanities_visits, 
  humanities_seconds, humanities_completed,
  noteworthy_videos, noteworthy_visits, 
  noteworthy_seconds, noteworthy_completed,
  finance_videos, finance_visits, 
  finance_seconds, finance_completed,
  testprep_videos, testprep_visits, 
  testprep_seconds, testprep_completed,
  interview_videos, interview_visits, 
  interview_seconds, interview_completed
  FROM user_video_top_category_stat
  WHERE start_dt = '${start_dt}' and end_dt = '${end_dt}'
) e on (a.user = e.user); 
