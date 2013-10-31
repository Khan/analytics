-- Create histogram of class size in order to validate > 10 number of students
--  definition of a teacher.

-- Depends on: student_teacher_current.q

DROP TABLE IF EXISTS coach_class_size;
CREATE EXTERNAL TABLE IF NOT EXISTS coach_class_size (
  coach STRING,
  student_count INT
) LOCATION 's3://ka-mapreduce/tmp/coach_class_size';

-- Only local files can be sourced as scripts
ADD FILE s3://ka-mapreduce/code/hive/student_teacher_current.q;

-- Only one of these should work, depending on which version of Hive we're on
SOURCE /mnt/var/lib/hive_081/downloaded_resources/student_teacher_current.q;
SOURCE /mnt/var/lib/hive_0110/downloaded_resources/student_teacher_current.q;

INSERT OVERWRITE TABLE coach_class_size
SELECT
  teacher, COUNT(1) AS student_count
FROM student_on_date
GROUP BY teacher;

INSERT INTO TABLE coach_class_size
SELECT
  coach, COUNT(1) AS student_count
FROM user_on_date
GROUP BY coach;

INSERT OVERWRITE TABLE class_size_histogram
SELECT
  coach_class_size.student_count, COUNT(1) AS teacher_count
FROM coach_class_size
GROUP BY coach_class_size.student_count;
