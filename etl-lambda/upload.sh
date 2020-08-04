zip -r9u function.zip *all the packagees, Metadata and the Script*
aws lambda update-function-code \
    --function-name  <lambda-function-name> \
    --zip-file fileb://function.zip
aws lambda update-function-configuration \
    --function-name <lambda-function-name> \
    --environment Variables="
    {
    FILEPATH_METADATA = '<path to Metadata>',
    DB_HOST = 'DB_HOST',
    DB_PORT = 'PORT',
    DB_NAME = 'DB_NAME',
    DB_USER = 'DB_USER',
    DB_PASSWORD = 'DB_PASSWORD',
    SAMPLING_FREQUENCY = 'SAMPLING_FREQUENCY'
    }"
rm function.zip
