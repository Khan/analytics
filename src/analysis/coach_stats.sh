#!/bin/bash

# This script executes BigQuery commands to compute Coach'n'class stats.
# You need the 'bq' command line tool installed, which can we set up with
# `pip install bigquery; bq init`

# TODO(user) Update this variable to point to the latest UserData backup
UserData='[jace.UserData_2013_10_23]'

# Number of self-identified teachers
bq query --format=pretty "
SELECT count(*) AS num_self_identified_teachers
FROM $UserData
WHERE teacher
"

# Number of users with a coach
bq query --format=pretty "
SELECT COUNT(1) AS num_users_with_a_coach FROM (
  SELECT user_id 
  FROM $UserData
  WHERE coaches IS NOT NULL and coaches != '' and coaches != user_id 
  GROUP BY user_id
)
"

# Number of coaches
bq query --format=pretty "
SELECT count(*) AS number_of_coaches
FROM (
  SELECT coaches
  FROM $UserData 
  GROUP BY coaches
)
"

# Now, before we compute 'cohort' (>=10 student) stats, we need
# create a couple intermediate tables, because BigQuery does 
# not allow us to do JOINs on repeated fields like 'coaches'

bq rm -f -t derived.user_coach_map
bq query --format=pretty --destination_table=derived.user_coach_map "
SELECT user_id, coaches as coach
FROM $UserData
WHERE coaches IS NOT NULL and coaches != '' and coaches != user_id 
"

bq rm -f -t derived.coach_summary
bq query --format=pretty --destination_table=derived.coach_summary "
SELECT coaches AS coach, COUNT(1) AS num_students 
FROM $UserData
WHERE coaches IS NOT NULL
GROUP BY coach
"

# Number of coaches of >= 10 accounts
bq query --format=pretty "
SELECT COUNT(*) AS number_of_coaches_gte10_students
FROM derived.coach_summary
WHERE num_students >= 10
"

# Number of students with a coaches of >= 10 accounts
bq query --format=pretty "
SELECT COUNT(*) AS num_users_with_coach_of_gte10_students
FROM (
  SELECT user.user_id
  FROM derived.user_coach_map AS user
  JOIN EACH derived.coach_summary AS coach
  ON user.coach = coach.coach
  WHERE coach.num_students >= 10
  GROUP BY user.user_id
) 
"

# De-duped number of teachers: self-idenfitied *UNION* num with >=10 students
bq query --format=pretty "
SELECT COUNT(1) AS num_deduped_teachers FROM (
  SELECT email
  FROM 
    (SELECT coach AS email FROM derived.coach_summary WHERE num_students >= 10),
    (SELECT user.email AS email FROM jace.UserData_2013_10_23 WHERE teacher)
  GROUP BY email
)
"
