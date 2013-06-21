HIVE_USER := hadoop
HIVE_MASTER := 23.23.92.49
PRIVATE_KEY := ~/.ssh/analytics.pem


upload: upload-s3 upload-hive

upload-hive:
	scp -i $(PRIVATE_KEY) hive/* $(HIVE_USER)@$(HIVE_MASTER):~/

upload-s3:
	python deploy.py -v code

geo-ip:
	# Moves fresh GeoIP database with pygeoip to s3 datastore for use with hive queries
	# You should only want to run this target if you are using geolocation library pygeoip
	#	and there has been an update to it or the GeoIP database
	# If the following command does not work refer to http://dev.maxmind.com/geoip/legacy/install/city
	# MaxMind recently announced V2 of their services, however, they don't have yet new offline database
	# 	for it
	wget -N -q http://geolite.maxmind.com/download/geoip/database/GeoLiteCity.dat.gz
	gunzip --stdout GeoLiteCity.dat.gz > GeoLiteCity.dat
	python deploy.py geoip

hadoop-ui:
	ssh -f -i $(PRIVATE_KEY) -l $(HIVE_USER) -N -L2001:localhost:9100 \
		$(HIVE_MASTER)
	@echo "Now hit localhost:2001 in your browser to see progress of Hadoop jobs."
