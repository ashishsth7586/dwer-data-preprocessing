zip -r9u function.zip urllib3 s3transfer pytz psycopg2_binary.libs psycopg2 pandas numpy.libs numpy jmespath docutils dateutil botocore boto3 bin six.py lambda_function.py __init__.py MetaData.json
aws lambda update-function-code \
    --function-name  dwerETL \
    --zip-file fileb://function.zip
aws lambda update-function-configuration \
    --function-name dwerETL \
    --environment Variables="
    {
    FILEPATH_METADATA = 'MetaData.json',
    DB_HOST = 'supplychain-db-prod.cluster-cpjaqbvxpfdq.ap-southeast-2.rds.amazonaws.com',
    DB_PORT = '5432',
    DB_NAME = 'dwer_prod_clone',
    DB_USER = 'postgres',
    DB_PASSWORD = 'DlRkndOPhbYOs4q7usMI',
    SAMPLING_FREQUENCY = 'H'
    }"
rm function.zip