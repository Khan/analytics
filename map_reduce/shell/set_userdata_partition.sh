#!/bin/sh
hadoop dfs -ls s3n://ka-mapreduce/entity_store/UserDataP/ |grep -v '_tmp' \
|tail -1 |perl -ne '($ds) = $_ =~ /dt=(\d{4}-\d{2}-\d{2})/; \
    print "SET hivevar:userdata_partition=${ds};";' > /home/hadoop/userdata_ver.q
hadoop dfs -rm s3n://ka-mapreduce/conf/userdata_ver.q
hadoop dfs -put /home/hadoop/userdata_ver.q s3n://ka-mapreduce/conf/userdata_ver.q
