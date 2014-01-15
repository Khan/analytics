/*
This query was created to gather insight about how students are submitting
problems either via the new Learning Dashboard or via the existing Tutorial
view.  This insight was presented in the "Content: State of the Union" document
created by Matt Wahl.

Further analysis of this data can be found in https://docs.google.com/a/khanacademy.org/spreadsheet/ccc?key=0Al7ZAdYc_lEVdDE4RUlXbXpUNDNaUVVoNDZyRy1vZmc&usp=drive_web#gid=4

It was found that an empty referrer string is not correlated to where users are
submitting exercises from:  https://docs.google.com/a/khanacademy.org/spreadsheet/ccc?key=0AhauPmyK0zDEdDMyRVl0eXA4YWJTTnJzLW9vSmtPU0E#gid=0

To run these queries:
1) Start up a Hive instance, note that you only need to initilize the website_request_logs table
    Which means you can start it with simply /home/hadoop/bin/hive -d INPATH=s3://ka-mapreduce/entity_store
    and then copy+paste from ka_hive_init.q the code that mounts the website_request_logs table.
2) Change the start_dt and end_dt vars to point to the dates that you require
3) Copy+paste a query at a time into the interpreter
4) Use a command like this to convert the raw output to a csv file:
cat -v /tmp/exercise_counts_2013_10/* | sed 's/,/_/g' | sed 's/\^A/,/g' > exercise_counts_2013_10.csv
5) Copy to your local machine with something like this:
elastic-mapreduce -j j-G85HR9YTY3GJ --get /home/hadoop/exercise_counts_2013_10.csv
*/

-- Produces results in [start_dt, end_dt)
-- TODO(mattfaus): Figure out how to get hivevar's working in the interactive
-- console.  It has a maddeningly different syntax vs. the command-line, and
-- I've found that it's easier to just change the hard-coded versions.
set hivevar:start_ dt='2013-10-01';
set hivevar:end_dt='2013-10-03';

INSERT OVERWRITE LOCAL DIRECTORY '/tmp/exercise_counts_2013_11'
SELECT
    -- The exercise name, such as addition_4
    exercise_submission_counts.exercise_name,
    -- Number of submissions that were sent from the dashboard
    sum(exercise_submission_counts.from_dashboard),
    -- Number of submissions that were sent from within a tutorial
    sum(exercise_submission_counts.non_dashboard),
    -- Number of empty referrer strings
    sum(exercise_submission_counts.empty_referer)
FROM (
    SELECT
        -- url is in the format:
        -- /api/v1/user/exercises/<exercise name>/problems/<problem number>/attempt
        split(url, "/")[5] AS exercise_name,  -- exercise name
        --split(url, "/")[7],  -- problem number

        -- Submissions from the dashboard have a referer that looks like
        -- http://www.khanacademy.org/#mission/math/task/2514976059
        -- http://www.khanacademy.org/mission/math/task/2512956152
        -- Well, actually the referer has changed over time, some look like:
        -- http://www.khanacademy.org/#task/2512956152
        -- http://www.khanacademy.org/task/2512956152
        -- And there are even some without a suffix at all
        -- http://www.khanacademy.org
        -- Furthermore, some referrers even have /v/ in them like they were
        -- submitted from a video page
        -- http://www.khanacademy.org/math/arithmetic/exponents-radicals/radical-radicals/v/square-roots-and-real-numbers
        -- The current explanation for this is that the JS magic we use
        -- to modify the browser's URL doesn't work for all students (who may
        -- be using an older browser), so if the URL contains either "/e/" or
        -- "/v/" it is assumed to be in the tutorial view, otherwise dashboard.

        -- instr returns "position" which is 1-based indexing
        IF(referer <> "-" AND length(trim(referer)) > 0 AND
            instr(referer, "/e/") = 0 AND instr(referer, "/v/") = 0, 1, 0) AS from_dashboard,

        -- Submissions from a tutorial have a referer that looks like
        -- http://www.khanacademy.org/math/trigonometry/functions_and_graphs/analyzing_functions/e/shifting_and_reflecting_functions
        IF(instr(referer, "/e/") > 0 OR instr(referer, "/v/") > 0, 1, 0) AS non_dashboard,

        IF(referer = "-" OR length(trim(referer)) = 0, 1, 0) AS empty_referer
    FROM website_request_logs
    WHERE method = "POST"
        -- instr returns "position" which is 1-based indexing, 0 means not found
        AND instr(url, "/api/v1/user/exercises/") > 0
        AND dt >= '2013-11-01'
        AND dt < '2013-12-01'
) exercise_submission_counts
GROUP BY exercise_submission_counts.exercise_name;


INSERT OVERWRITE LOCAL DIRECTORY '/tmp/user_counts_2013_11'
SELECT
    -- Number of unique users that have submitted at least 1 attempt
    count(*) AS unique_users,

    -- Number of unique users that have submitted an attempt on the dashboard
    sum(total_counts.math_on_dashboard),

    -- Number of unique users that have submitted a math problem within a tutorial
    sum(total_counts.math_non_dashboard),

    -- Number of unique users that have submitted a NON-math problem within a tutorial
    sum(total_counts.non_math_non_dashboard),

    -- Number of empty referers
    sum(total_counts.empty_referer)
FROM (
    SELECT
        user_counts.bingo_id,
        max(user_counts.math_on_dashboard) AS math_on_dashboard,
        max(user_counts.math_non_dashboard) AS math_non_dashboard,
        max(user_counts.non_math_non_dashboard) AS non_math_non_dashboard,
        max(user_counts.empty_referer) AS empty_referer
    FROM (
        SELECT
            bingo_id,
            -- See the explanations of the url parsing in the query above
            -- instr returns "position" which is 1-based indexing
            IF(referer <> "-" AND length(trim(referer)) > 0 AND
                instr(referer, "/e/") = 0 AND instr(referer, "/v/") = 0, 1, 0) AS math_on_dashboard,
            IF((instr(referer, "/e/") > 0 OR instr(referer, "/v/") > 0)
                AND instr(referer, "/math/") > 0, 1, 0) AS math_non_dashboard,
            IF((instr(referer, "/e/") > 0 OR instr(referer, "/v/") > 0)
                AND instr(referer, "/math/") = 0, 1, 0) AS non_math_non_dashboard,
            IF(referer = "-" OR length(trim(referer)) = 0, 1, 0) AS empty_referer
        FROM website_request_logs
        WHERE method = "POST"
            -- instr returns "position" which is 1-based indexing, 0 means not found
            AND instr(url, "/api/v1/user/exercises/") > 0
            AND bingo_id IS NOT NULL
            AND dt >= '2013-11-01'
            AND dt < '2013-12-01'
    ) user_counts
    GROUP BY user_counts.bingo_id
) total_counts;

