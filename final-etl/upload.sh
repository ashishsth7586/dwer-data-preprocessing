zip -r9u <-All python dependencies-> lambda_function.py
aws lambda update-function-code \
    --function-name  <lambda-function-name> \
    --zip-file fileb://function.zip
aws lambda update-function-configuration \
    --function-name <lambda-function-name> \
    --environment Variables="
    {
    DB_HOST = '',
    DB_PORT = '5432',
    DB_NAME = '',
    DB_USER = '',
    DB_PASSWORD = '',
    REGION_NAME = '',
    AWS_KEY_ACCESS = '',
    AWS_KEY_SECRET = '',
    BUCKET_NAME = '',
    METADATA_JSON_KEY = '',
    TOPIC_ARN_SNS = '',
    SAMPLING_FREQUENCY = 'H',
    DEBUG=False
    }"
rm function.zip
