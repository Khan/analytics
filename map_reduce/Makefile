HIVE_USER := hadoop
HIVE_MASTER := 107.23.8.57
PRIVATE_KEY := ~/.ssh/analytics.pem


# TODO(david): Yun-Fang suggested that we move our files in map_reduce into py/
#     and hive/ subdirectories.
upload: upload-s3 upload-hive

upload-hive:
	scp -i $(PRIVATE_KEY) hive/* $(HIVE_USER)@$(HIVE_MASTER):~/

upload-s3:
	s3cmd put hive/* s3://ka-mapreduce/code/hive/
	s3cmd put py/* s3://ka-mapreduce/code/py/

hadoop-ui:
	ssh -f -i $(PRIVATE_KEY) -l $(HIVE_USER) -N -L2001:localhost:9100 \
		$(HIVE_MASTER)
	@echo "Now hit localhost:2001 in your browser to see progress of Hadoop jobs."
