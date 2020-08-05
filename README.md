# Weather Station Data Pre-Processing, Perth, Australia

This project consists of a python script deployed in AWS Lambda which is triggered when a file is uploaded to S3 Bucket (Supplychain-test) every hour via FTP . 
For each uploaded file, a single instance of lambda function is triggered. 
The script takes in the file and its metadata, process it to our custom standard format based on POSTGRESSQL schema(tablenames) which is stored in MetaData.json, 
generate the hourly average, deletes the old records from the database if present and
loads the data.
Note: Meteorological Data and Air Quality data file received may present in seperate files or together. Based on this,
we have to upload the file to the db.

# upload.sh
This is a bash script (AWS CLI), that zips all the files including the python packages and loads to AWS lambda.
