-- Hive script to join the user_engagement_dataset for 
-- engagement prediction
-- month_1: month date stamp YYYY-mm for predictors
-- month_2: month date stamp YYYY-mm for target variables
ALTER TABLE user_engagement_dataset RECOVER PARTITIONS;
INSERT OVERWRITE DIRECTORY 
's3://ka-mapreduce/tmp/joined_user_engagement_training_20120506' 
SELECT a.*, b.* 
FROM (
  SELECT
  user,
  user_id,
  user_email,
  joined_date,
  total_visits,
  video_visits,
  exercise_visits,
  feedback_visits,
  video_exercise_visits,
  video_feedback_visits,
  exercise_feedback_visits,
  timespent_exercise,
  num_exercise,
  num_proficient,
  avg_accuracy, 
  timespent_video,
  num_videos, 
  num_completed, 
  num_videos_commented,
  num_comments,
  num_questions, 
  num_answers, 
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
  FROM user_engagement_dataset 
  WHERE start_dt = '${month_1}-01' AND type = 'registered'
) a LEFT OUTER JOIN (
  SELECT
  user,
  user_id,
  user_email,
  joined_date,
  total_visits,
  video_visits,
  exercise_visits,
  feedback_visits,
  video_exercise_visits,
  video_feedback_visits,
  exercise_feedback_visits,
  timespent_exercise,
  num_exercise,
  num_proficient,
  avg_accuracy, 
  timespent_video,
  num_videos, 
  num_completed, 
  num_videos_commented,
  num_comments,
  num_questions, 
  num_answers, 
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
  FROM user_engagement_dataset 
  WHERE start_dt = '${month_2}-01' AND type = 'registered'
) b ON (a.user = b.user);

