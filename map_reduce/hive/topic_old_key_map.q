-- Create mapping from topic old_key_name to its title
-- Used by badges dashboard

-- Some badges names are of a form:
--  "topic_exercise_LjZH9OiVLkOOeF4QDuyKlI1QzI8U9dRelqF81GWH",
-- Where long string after "topic_exercise_" is a old_key_name
--  property of a topic. This table allows us to map these keys
--  to topic names in the badges dashboard.
-- Result set is roughly 2k rows and should stay constant.
-- Since it is no longer a recommended approach to use
--  old_key_name topic property this table will likely become obsolete soon.

-- TODO(robert): This table is only necessary as long as
--  badges names are not self explantory. In this case these
--  are topic exercise badges which refer to topic's old_key_name.

INSERT OVERWRITE TABLE topic_old_key_name
SELECT
  j.title, j.standalone_title, j.old_key_name
FROM Topic t
LATERAL VIEW json_tuple(t.json, "title", "standalone_title", "old_key_name") j AS
    title, standalone_title, old_key_name
WHERE j.old_key_name IS NOT NULL;
