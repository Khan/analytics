HIVE_USER := hadoop
HIVE_MASTER := 107.23.8.57
PRIVATE_KEY := ~/.ssh/analytics.pem


upload: upload-s3 upload-hive

upload-hive:
	scp -i $(PRIVATE_KEY) hive/* $(HIVE_USER)@$(HIVE_MASTER):~/

upload-s3:
	python deploy.py

hadoop-ui:
	ssh -f -i $(PRIVATE_KEY) -l $(HIVE_USER) -N -L2001:localhost:9100 \
		$(HIVE_MASTER)
	@echo "Now hit localhost:2001 in your browser to see progress of Hadoop jobs."
