#!/bin/sh

start_dt=$1
end_dt=$2
branch=dev

if [ $# -ne 2 ];
then
    echo "Usage: `basename $0` [start_date] [end_date]";
    exit 1
fi

elastic-mapreduce --create \
        --name "Video Recommendation ${branch} ${start_dt} - ${end_dt}" \
        --num-instances 3 \
        --instance-type m1.large \
        --hive-script \
        --arg s3://ka-mapreduce/code/dev/hive/video_recommendation.q \
        --args -i,s3://ka-mapreduce/code/hive/ka_hive_init.q \
        --args -d,INPATH=s3://ka-mapreduce/entity_store \
        --args -d,branch=${branch} \
        --args -d,suffix=${start_dt}-${end_dt} \
        --args -d,start_dt=${start_dt} \
        --args -d,end_dt=${end_dt}
