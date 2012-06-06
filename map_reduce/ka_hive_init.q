-- Initialization script for hive to recover tables mirroring GAE entities. Run
-- "hive -d INPATH=s3://ka-mapreduce/entity_store -i s3://ka-mapreduce/code/hive/ka_hive_init.q"
-- for your interactive hive shells.
CREATE EXTERNAL TABLE IF NOT EXISTS Exercise (
    key string, json string
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LOCATION '${INPATH}/Exercise';


CREATE EXTERNAL TABLE IF NOT EXISTS Feedback (
    key string, json string
  )
  PARTITIONED BY (dt string)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LOCATION '${INPATH}/Feedback';
ALTER TABLE Feedback RECOVER PARTITIONS;


CREATE EXTERNAL TABLE IF NOT EXISTS FeedbackVote (
    key string, json string
  )
  PARTITIONED BY (dt string)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LOCATION '${INPATH}/FeedbackVote';
ALTER TABLE FeedbackVote RECOVER PARTITIONS;


CREATE EXTERNAL TABLE IF NOT EXISTS ProblemLog (
    user string, json string
  )
  PARTITIONED BY (dt string)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LOCATION '${INPATH}/ProblemLog';
ALTER TABLE ProblemLog RECOVER PARTITIONS;


CREATE EXTERNAL TABLE IF NOT EXISTS StackLog (
    user string, json string
  )
  PARTITIONED BY (dt string)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LOCATION '${INPATH}/StackLog';
ALTER TABLE StackLog RECOVER PARTITIONS;


CREATE EXTERNAL TABLE IF NOT EXISTS Topic (
    key string, json string
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LOCATION '${INPATH}/Topic';


CREATE EXTERNAL TABLE IF NOT EXISTS UserBadge (
    user string, json string
  )
  PARTITIONED BY (dt string)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LOCATION '${INPATH}/UserBadge';
ALTER TABLE UserBadge RECOVER PARTITIONS;


CREATE EXTERNAL TABLE IF NOT EXISTS UserData (
    key string, json string
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LOCATION '${INPATH}/UserData';


CREATE EXTERNAL TABLE IF NOT EXISTS Video (
    key string, json string
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LOCATION '${INPATH}/Video';


CREATE EXTERNAL TABLE IF NOT EXISTS VideoLog (
    user string, json string
  )
  PARTITIONED BY (dt string)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LOCATION '${INPATH}/VideoLog';
ALTER TABLE VideoLog RECOVER PARTITIONS;
