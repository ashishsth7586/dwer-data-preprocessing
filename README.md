# Weather Station Data Pre-Processing, Perth, Australia

This project consists of a python script deployed in AWS Lambda which is triggered when weather data files are uploaded to S3 Bucket(Sectorinsight/hubs/) every hour via a FTP Server. 
For every uploaded file, a single instance of lambda function is triggered. 
The script takes in the file full-path and its metadata (translations or columnnames mappings) loaded from MetaData.json, process it to our custom standard format, 
generates the hourly average, deletes the old records from the database if present and finally,
loads the data.

__Note__: Station may send Meteorological Data and Air Quality data file in a single file combined or seperately. Based on this condition,
we have to upload the file to the AWS RDS.

# upload.sh
This is a bash script (AWS CLI), that zips all the files including the python packages and loads to AWS lambda.

# Tech Stack:
- Python
- Bash (move raw files to S3 and FTP file watcher) 
- AWS S3 (store raw files)
- AWS Lambda (preprocessing script)
- AWS Simple Notification System (sends email when error occurs)
- Cloudwatch (logs)
- FTP Server (to receive raw files from station)
- Elastic Search
