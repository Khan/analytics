-- Query to produce time series of students and teachers using the site.
-- TODO(robert): Results produced by these queries are fundamentally flawed.
-- We have to record date of change of coach to be able to produce
-- accurate report

-- Arguments:
--   end_dt: exclusive end date in YYYY-MM-DD format

-- Computed Metrics:
--   Teacher: Coach who has at least 10 students.
--   Active Teacher: Teacher with at least 10 active students.
--   Student: User who has a teacher.
--   Active Student: Student who completed an activity in last 28 days.

-- Only local files can be sourced as scripts
ADD FILE s3://ka-mapreduce/code/hive/student_teacher_current.q;
SOURCE /mnt/var/lib/hive_0110/downloaded_resources/student_teacher_current.q;

DROP TABLE IF EXISTS active_user_on_date;
CREATE EXTERNAL TABLE IF NOT EXISTS active_user_on_date (
  user STRING,
  coach STRING,
  joined_on STRING,
  dt STRING
) LOCATION 's3://ka-mapreduce/tmp/active_user_on_date';

DROP TABLE IF EXISTS active_student_on_date;
CREATE EXTERNAL TABLE IF NOT EXISTS active_student_on_date (
  student STRING,
  teacher STRING,
  joined_on STRING,
  dt STRING
) LOCATION 's3://ka-mapreduce/tmp/active_student_on_date';

-- Find all active students.
-- Active student is a user who performed an action,
-- as defined by user_daily_activity, in last 28 days
INSERT OVERWRITE TABLE active_student_on_date
SELECT
  student_on_date.student,
  student_on_date.teacher,
  student_on_date.dt,
  user_daily_activity.dt
FROM user_daily_activity
JOIN student_on_date
ON user_daily_activity.user = student_on_date.student
WHERE user_daily_activity.dt < '${end_dt}' AND
  user_daily_activity.dt >= student_on_date.dt
GROUP BY student_on_date.student, student_on_date.teacher,
    student_on_date.dt, user_daily_activity.dt;

-- Find all active users with coaches.
-- Active user is a user who performed an action,
-- as defined by user_daily_activity, in last 28 days
INSERT OVERWRITE TABLE active_user_on_date
SELECT
  user_on_date.user,
  user_on_date.coach,
  user_on_date.dt,
  user_daily_activity.dt
FROM user_daily_activity
JOIN user_on_date
ON user_daily_activity.user = user_on_date.user
WHERE user_daily_activity.dt < '${end_dt}' AND
  user_daily_activity.dt >= user_on_date.dt
GROUP BY user_on_date.user, user_on_date.coach,
    user_on_date.dt, user_daily_activity.dt;


-- Merge all the results together
--  * number of teachers
--  * number of students
--  * number of coaches (with less than 10 students)
--  * number of users with a coach (in a group of less than 10 pupils)
--  * number of active teachers
--  * number of active students
--  * number of active coaches (with less than 10 students)
--  * number of active users with a coach (in a group of less than 10 pupils)
--  * number of teachers who in last 28 days visited class_profile
--  * number of coaches (with less than 10 students)
--     who in last 28 days visited class_profile
-- Refer to map_reduce/py/coach_reduce.py and beginning of
-- this file for details
INSERT OVERWRITE TABLE student_teacher_count
SELECT t_nr.teacher_count, st_nr.student_count, c_nr.coach_count,
    cu_nr.coach_user_count, act_t.active_teachers,
    act_st.active_students, act_c.active_coaches,
    act_cu.active_coach_users, heng_t.highly_engaged_teachers,
    heng_st.highly_engaged_students, heng_c.highly_engaged_coaches,
    heng_cu.highly_engaged_coach_users, lng_t.long_term_active_teachers,
    lng_st.long_term_active_students, lng_c.long_term_active_coaches,
    lng_cu.long_term_active_coach_users, v_t.visiting_teachers,
    v_c.visiting_coaches, t_nr.dt
FROM (
  FROM (
      SELECT COUNT(1) as teacher_count, dt
      FROM teacher_on_date
      GROUP BY dt
      ORDER BY dt
  ) t_date
  REDUCE t_date.teacher_count, t_date.dt
  USING 'coach_reduce.py count "${end_dt}"' AS teacher_count, dt
) t_nr
JOIN (
    FROM (
        SELECT COUNT(1) as student_count, dt
        FROM student_on_date
        GROUP BY dt
        ORDER BY dt
    ) st_date
    REDUCE st_date.student_count, st_date.dt
    USING 'coach_reduce.py count "${end_dt}"' AS student_count, dt
) st_nr
ON t_nr.dt = st_nr.dt
JOIN (
    FROM (
        SELECT COUNT(1) as coach_count, dt
        FROM coach_on_date
        GROUP BY dt
        ORDER BY dt
    ) c_date
    REDUCE c_date.coach_count, c_date.dt
    USING 'coach_reduce.py count "${end_dt}"' AS coach_count, dt
) c_nr
ON t_nr.dt = c_nr.dt
JOIN (
    FROM (
        SELECT COUNT(1) as coach_user_count, dt
        FROM user_on_date
        GROUP BY dt
        ORDER BY dt
    ) u_date
    REDUCE u_date.coach_user_count, u_date.dt
    USING 'coach_reduce.py count "${end_dt}"' AS coach_user_count, dt
) cu_nr
ON t_nr.dt = cu_nr.dt
LEFT OUTER JOIN (
    FROM (
        SELECT student, teacher, dt
        FROM active_student_on_date
        ORDER BY dt, teacher
    ) teacher_active_count
    REDUCE teacher_active_count.student,
      teacher_active_count.teacher, teacher_active_count.dt
    USING 'coach_reduce.py active-teacher "${end_dt}" 10 1' AS active_teachers, dt
) act_t
ON t_nr.dt = act_t.dt
LEFT OUTER JOIN (
    FROM (
        SELECT student, dt
        FROM active_student_on_date
        ORDER BY dt
    ) active_count
    REDUCE active_count.student, active_count.dt
    USING 'coach_reduce.py active-student "${end_dt}" 1' AS active_students, dt
) act_st
ON t_nr.dt = act_st.dt
LEFT OUTER JOIN (
    FROM (
        SELECT user, coach, dt
        FROM active_user_on_date
        ORDER BY dt, coach
    ) coach_active_count
    REDUCE coach_active_count.user,
      coach_active_count.coach, coach_active_count.dt
    USING 'coach_reduce.py active-teacher "${end_dt}" 1 1' AS active_coaches, dt
) act_c
ON t_nr.dt = act_c.dt
LEFT OUTER JOIN (
    FROM (
        SELECT user, dt
        FROM active_user_on_date
        ORDER BY dt
    ) active_user_count
    REDUCE active_user_count.user, active_user_count.dt
    USING 'coach_reduce.py active-student "${end_dt}" 1' AS active_coach_users, dt
) act_cu
ON t_nr.dt = act_cu.dt
LEFT OUTER JOIN (
    FROM (
        SELECT student, teacher, dt
        FROM active_student_on_date
        ORDER BY dt, teacher
    ) teacher_active_count
    REDUCE teacher_active_count.student,
      teacher_active_count.teacher, teacher_active_count.dt
    USING 'coach_reduce.py active-teacher "${end_dt}" 10 4' AS
      highly_engaged_teachers, dt
) heng_t
ON t_nr.dt = heng_t.dt
LEFT OUTER JOIN (
    FROM (
        SELECT student, dt
        FROM active_student_on_date
        ORDER BY dt
    ) active_count
    REDUCE active_count.student, active_count.dt
    USING 'coach_reduce.py active-student "${end_dt}" 4' AS
      highly_engaged_students, dt
) heng_st
ON t_nr.dt = heng_st.dt
LEFT OUTER JOIN (
    FROM (
        SELECT user, coach, dt
        FROM active_user_on_date
        ORDER BY dt, coach
    ) coach_active_count
    REDUCE coach_active_count.user,
      coach_active_count.coach, coach_active_count.dt
    USING 'coach_reduce.py active-teacher "${end_dt}" 1 4' AS
     highly_engaged_coaches, dt
) heng_c
ON t_nr.dt = heng_c.dt
LEFT OUTER JOIN (
    FROM (
        SELECT user, dt
        FROM active_user_on_date
        ORDER BY dt
    ) active_user_count
    REDUCE active_user_count.user, active_user_count.dt
    USING 'coach_reduce.py active-student "${end_dt}" 4' AS
      highly_engaged_coach_users, dt
) heng_cu
ON t_nr.dt = heng_cu.dt
LEFT OUTER JOIN (
    FROM (
        SELECT student, teacher, dt
        FROM active_student_on_date
        WHERE DATE_ADD(active_student_on_date.joined_on, 28) <
            active_student_on_date.dt
        ORDER BY dt, teacher
    ) long_term_teacher_active_count
    REDUCE long_term_teacher_active_count.student,
      long_term_teacher_active_count.teacher, long_term_teacher_active_count.dt
    USING 'coach_reduce.py active-teacher "${end_dt}" 10 1' AS
      long_term_active_teachers, dt
) lng_t
ON t_nr.dt = lng_t.dt
LEFT OUTER JOIN (
    FROM (
        SELECT student, dt
        FROM active_student_on_date
        WHERE DATE_ADD(active_student_on_date.joined_on, 28) <
            active_student_on_date.dt
        ORDER BY dt
    ) long_term_active_count
    REDUCE long_term_active_count.student, long_term_active_count.dt
    USING 'coach_reduce.py active-student "${end_dt}" 1' AS
      long_term_active_students, dt
) lng_st
ON t_nr.dt = lng_st.dt
LEFT OUTER JOIN (
    FROM (
        SELECT user, coach, dt
        FROM active_user_on_date
        WHERE DATE_ADD(active_user_on_date.joined_on, 28) <
            active_user_on_date.dt
        ORDER BY dt, coach
    ) long_term_coach_active_count
    REDUCE long_term_coach_active_count.user,
      long_term_coach_active_count.coach, long_term_coach_active_count.dt
    USING 'coach_reduce.py active-teacher "${end_dt}" 1 1' AS
      long_term_active_coaches, dt
) lng_c
ON t_nr.dt = lng_c.dt
LEFT OUTER JOIN (
    FROM (
        SELECT user, dt
        FROM active_user_on_date
        WHERE DATE_ADD(active_user_on_date.joined_on, 28) <
            active_user_on_date.dt
        ORDER BY dt
    ) long_term_active_user_count
    REDUCE long_term_active_user_count.user, long_term_active_user_count.dt
    USING 'coach_reduce.py active-student "${end_dt}" 1' AS
      long_term_active_coach_users, dt
) lng_cu
ON t_nr.dt = lng_cu.dt
LEFT OUTER JOIN (
    FROM (
        SELECT teacher_on_date.teacher AS teacher,
          daily_class_profile_visits.dt AS dt
        FROM teacher_on_date
        JOIN user_coach_date b
        ON teacher_on_date.teacher = b.coach
        JOIN daily_class_profile_visits
        ON daily_class_profile_visits.bingo_id = b.gae_bingo_identity
        GROUP BY teacher_on_date.teacher,
          daily_class_profile_visits.dt
        ORDER BY dt
    ) teacher_visit
    REDUCE teacher_visit.teacher, teacher_visit.dt
    USING 'coach_reduce.py active-student "${end_dt}" 1' AS visiting_teachers, dt
) v_t
ON t_nr.dt = v_t.dt
LEFT OUTER JOIN (
    FROM (
        SELECT coach_on_date.coach AS coach,
          daily_class_profile_visits.dt AS dt
        FROM coach_on_date
        JOIN user_coach_date b
        ON coach_on_date.coach = b.coach
        JOIN daily_class_profile_visits
        ON daily_class_profile_visits.bingo_id = b.gae_bingo_identity
        GROUP BY coach_on_date.coach,
            daily_class_profile_visits.dt
        ORDER BY dt
    ) coach_visit
    REDUCE coach_visit.coach, coach_visit.dt
    USING 'coach_reduce.py active-student "${end_dt}" 1' AS visiting_coaches, dt
) v_c
ON t_nr.dt = v_c.dt;
