# dwer-data-preprocessing

This is a lambda function, python script which is triggered when a file is uploaded to S3 Bucket (Supplychain-test). 
For each file uploaded, a single instance of lambda function is triggered. 
The script takes in the file, process it to our custom standard format based on POSTGRESSQL schema(tablenames), 
generate the hourly average of the data, deletes the older records from the database if present and
loads the data to db.
Note: The station file may consists of Meteorological Data and Air Quality data seperately and together. Based on this,
we have to upload the file to the db.
