import os
import json
import time
import logging
from datetime import datetime

import botocore
import boto3
import psycopg2
import psycopg2.extras as extras
import pandas as pd
import numpy as np
from botocore.exceptions import ClientError


# Variable Initialization
# get values from Environment Variable (DB, AWS)
# Instantiate Logger, SNS, S3 object

# DB
DB_HOST: str = os.environ['DB_HOST']
DB_PORT: str = os.environ['DB_PORT']
DB_NAME: str = os.environ['DB_NAME']
DB_USER: str = os.environ['DB_USER']
DB_PASSWORD: str = os.environ['DB_PASSWORD']

# AWS
REGION_NAME: str = os.environ['REGION_NAME']
AWS_KEY_ACCESS: str = os.environ['AWS_KEY_ACCESS']
AWS_KEY_SECRET: str = os.environ['AWS_KEY_SECRET']
BUCKET_NAME: str = os.environ['BUCKET_NAME']
METADATA_JSON_KEY: str = os.environ.get('METADATA_JSON_KEY', 'data/MetaData.json')
TOPIC_ARN_SNS: str = os.environ['TOPIC_ARN_SNS']

SAMPLING_FREQUENCY: str = os.environ['SAMPLING_FREQUENCY']
DEBUG: bool = os.environ['DEBUG']

logger = logging.getLogger(__name__)
sns_client = boto3.client(
    'sns',
    region_name=REGION_NAME,
    aws_access_key_id=AWS_KEY_ACCESS,
    aws_secret_access_key=AWS_KEY_SECRET
)
s3_client = boto3.client(
    's3',
    region_name=REGION_NAME,
    aws_access_key_id=AWS_KEY_ACCESS,
    aws_secret_access_key=AWS_KEY_SECRET
)

# Set logging level according to the DEBUG
# https://docs.python.org/3/library/logging.html#logging-levels
if DEBUG:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)

# Database connection parameters
params_dict = {
    "host": DB_HOST,
    "port": DB_PORT,
    "database": DB_NAME,
    "user": DB_USER,
    "password": DB_PASSWORD
}
# python list of Meteorological and Air Quality columns in Database.
met_columnnames = ['datetime', 'time_key', 'station', 'interval_length', 'sp04', 'sp10', 'gu04', 'gu10', 'dn04', 'dn10', 'sg04', 'sg10', 'at02', 'at10', 'delt', 'rh02', 'rh10', 'pres', 'rain', 'swin', 'swout', 'iwin', 'iwout', 'grad', 'nrad']
aqm_columnnames = ['datetime', 'time_key', 'station', 'interval_length', 'pm2_5', 'pm10', 'pmcrs', 'so2', 'co', 'no', 'no2', 'nox', 'o3']
# Aggregate functions: either sum or mean
column_agg_mapping = {
    'sp04': 'mean', 'sp10': 'mean', 'gu04': 'mean', 'gu10': 'mean', 
    'dn04': 'mean', 'dn10': 'mean', 'sg04': 'mean', 'sg10': 'mean',
    'at02': 'mean', 'at10': 'mean', 'delt': 'mean', 'rh02': 'mean',
    'rh10': 'mean', 'pres': 'mean', 'rain': 'sum', 'swin': 'mean',
    'swout': 'mean', 'lwin': 'mean', 'lwout': 'mean', 'grad': 'mean', 
    'nrad': 'mean', 'pm2_5': 'mean', 'pm10': 'mean', 'pmcrs': 'mean',
    'so2': 'mean', 'co': 'mean', 'no': 'mean', 'no2': 'mean',
    'nox': 'mean', 'o3': 'mean'    
}

# Error Handling Class
class ErrorMessageHandling:
    """
    DOCSTRING: add later ######
    """
    def __init__(self, sns, topic_arn, logger):
        self.sns = sns
        self.topic_arn = topic_arn
        self.logger = logger
        self.message = ''
    
    @property
    def publish_to_sns(self):
        try:
            self.sns.publish(
                TopicArn=self.topic_arn,
                Message=self.message,
                Subject='Error Notification'
            )
        except:
            logger.error("Failed to publish message to SNS.")
    
    @property
    def log_as_error(self):
        logger.error(self.message)
    
    @property
    def log_as_info(self):
        logger.info(self.message)

    @property
    def log_as_debug(self):
        logger.debug(self.message)
    
    @property
    def log_as_warning(self):
        logger.warning(self.message)
    
    def __repr__(self):
        return f'<ErrorMessageHandling {self.message}>'

msg_obj = ErrorMessageHandling(sns_client, TOPIC_ARN_SNS, logger)

# Other helper functions
def load_metadata(obj):
    """
    loads metadata from s3 bucket
    """
    import io
    data = json.load(io.BytesIO(obj['Body'].read()), encoding='utf8')
    return data

def get_translation_from_filename(filename, metadata):
    """
    returns standard column names mapping of a file.
    """
    if isinstance(metadata, dict):
        if 'translations' in metadata:
            translations = metadata['translations']

            for i in range(len(translations)):
                if translations[i]['filename'].lower() == filename.lower():
                    return translations[i]['columnnames']
    else:
        msg_obj.message = 'Type Mismatched: Metadata is not a valid Dictionary Object.'
        msg_obj.log_as_error
        msg_obj.publish_to_sns

def get_metadata_from_filename(filename, metadata):
    """
    returns entire metadata of a file.
    """
    if isinstance(metadata, dict):
        if 'translations' in metadata:
            translations = metadata['translations']

            for i in range(len(translations)):
                if translations[i]['filename'].lower() == filename.lower():
                    return translations[i]
    else:
        msg_obj.message = 'Type Mismatched: Metadata is not a valid Dictionary Object.'
        msg_obj.log_as_error
        msg_obj.publish_to_sns

def connect_db(params_dict):
    """
    Connect to the PostgreSQL database server
    returns connection instance.
    """
    conn = None
    try:
        msg_obj.message = 'Connecting to the PostgreSQL Database...'
        msg_obj.log_as_debug
        conn = psycopg2.connect(**params_dict)
    except (Exception, psycopg2.DatabaseError) as error:
        msg_obj.message = "An error occurred while establishing connection to database."
        msg_obj.log_as_error
        if DEBUG:
            raise
    return conn

def split_met_aqm(df, met_columnnames, aqm_columnnames):
    """
    splits combined dataframe into Meteorological and Air quality
    dataframes
    """
    cols = list(df.columns)

    met_columns, aqm_columns = [], []

    for column in cols:
        if column in met_columnnames:
            met_columns.append(column)
        if column in aqm_columnnames:
            aqm_columns.append(column)

    return df[met_columns], df[aqm_columns]

def predelete_records(conn, min_datetime, max_datetime, tablename, station_id):
    """
    predeletes the existing records from database based on
    min_datetime and max_datetime of calculated hourly average
    records.
    """
    if conn is not None:        
        query = "DELETE FROM " + "core_data." + tablename + " WHERE station=" + station_id + " AND datetime>=" + "'" + min_datetime + "'" + " AND datetime<=" + "'" + max_datetime + "'"
        cur = conn.cursor()    
        try:
            cur.execute(query)
            conn.commit()

            count = cur.rowcount
            msg_obj.message = f"{count} records deleted."
            msg_obj.log_as_info

        except (Exception, psycopg2.DatabaseError) as error:  
            msg_obj.message = "An error occurred while establishing connection to database."
            msg_obj.log_as_error                   
            conn.rollback()         
            if DEBUG:
                raise
        finally:
            if cur:
                cur.close()
                
def get_min_max_datetime(df):
    """
    returns minimum and maximum datetime 
    where `datetime` is time-series index.
    """
    return df.index.min(), df.index.max()

def execute_batch_insertion(conn, min_datetime, max_datetime, df, fieldnames, tablename, station_id, page_size=100):
    """
    using psycopg2.extras.execute_batch() to insert the dataframe
    """
    predelete_records(conn, str(min_datetime), str(max_datetime), str(tablename), str(station_id))

    tuples = [tuple(x) for x in df.to_numpy()]
    
    query = 'INSERT INTO ' + 'core_data.' + tablename + '(' + ','.join(fieldname for fieldname in fieldnames) + ') VALUES(' + ','.join(['%s'] * len(fieldnames)) + ');'
    
    cur = conn.cursor()
    try:
        extras.execute_batch(cur, query, tuples, page_size)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        msg_obj.message = "An error occurred while establishing connection to database."
        msg_obj.log_as_error                   
        conn.rollback()         
        if DEBUG:
            raise
    finally:
        msg_obj.message = "Successfully loaded all data to PostgreSQL"
        msg_obj.log_as_info
        if cur:
            cur.close()

def process_dataframe(df, columnnames_tx, csv_file):
   
    if 'Date_Time.1' in list(df.columns):
        df = df.drop('Date_Time.1', axis=1)
    
    # check whether any expected (metadata) columns are missing from  the source dataframe
    missing_columns = []
    orig_columns = list(df.columns)
    
    for value in list(columnnames_tx.keys()):
        if value not in orig_columns:
            missing_columns.append(value)
            
    if len(missing_columns) > 0:
        msg_obj.message = f"Column(s): '{missing_columns}' missing  in file: '{csv_file}'. Aborting!"
        msg_obj.publish_to_sns
        msg_obj.log_as_error
        # return empty dataframe
        return pd.DataFrame()

    # rename columns as in MetaData
    df.rename(columns=columnnames_tx, inplace=True)
    
    std_columns = list(columnnames_tx.values())
    renamed_columns = list(df.columns)
    
    # drop duplicate columns
    df = df.loc[:, ~df.columns.duplicated()]
    
    # drop columns if not present in Metadata
    for value in renamed_columns:
        if value not in std_columns:
            df = df.drop(value, axis=1)
            msg_obj.message =f"Successfully dropped unrecognized column: {value}"
            msg_obj.log_as_info
    return df

def load_to_db(filename, metadata, met_tablename='dwer_met', aqm_tablename='dwer_aqm'):

    # extract only filename from the file_path
    splitted_filenames = filename.split('_')
    date, extension = splitted_filenames[-1].split('.')
    filename_without_date = '_'.join(splitted_filenames[0:-1]) + '.' + extension
    csv_file = filename_without_date.split('/')[-1]

    # get the column names mapping from metadata
    columnnames_tx = get_translation_from_filename(csv_file, metadata)
    
    # if columnnames_tx is None abort.
    if not columnnames_tx:
        msg_obj.message = f"File: {csv_file} was not found in metadata! Aborting"
        msg_obj.publish_to_sns
        msg_obj.log_as_error
        return        
    
    # Get source data and process it
    df = pd.read_csv(filename)
    df = process_dataframe(df, columnnames_tx, csv_file)
    if df.empty:
        return

    # generate column aggregate mapping for a given file
    col_agg_mappings = { key: column_agg_mapping[key] for key in list(columnnames_tx.values())[1:]}
       
    # set `datetime` as the index
    data_series = pd.notnull(df["datetime"])
    df['datetime'] = df['datetime'][data_series].apply(lambda x: datetime.strptime(x, '%d%m%Y %H%M'))
    indexes = pd.DatetimeIndex(df['datetime'])
    df = df.set_index(indexes)

    # generate hourly average records
    try:
        df_hourly = df.resample(SAMPLING_FREQUENCY).agg(col_agg_mappings)        
    except pd.core.base.SpecificationError:
        msg_obj.message = "An error occurred while resampling dataframe."
        msg_obj.log_as_error
        if DEBUG:
            raise    
    else:
        if 'rain' in list(df_hourly.columns):
            df_hourly.loc[df_hourly['rain'] == 0, 'rain'] = None

        # drop records if all the records are null except datetime
        df_hourly = df_hourly.dropna(how='all', axis=0)

        # add sampled datetime index to new column    
        df_hourly.insert(0, 'datetime', df_hourly.index)

        # replace NaN to Null(None)
        df_hourly = df_hourly.replace({np.nan: None})  
        # df_hourly = df_hourly.where(pd.notnull(df_hourly), None)

        if df_hourly.empty:
            msg_obj.message = f"File: {csv_file} is empty!"
            msg_obj.log_as_error
            msg_obj.publish_to_sns
            return
        
        min_datetime, max_datetime = get_min_max_datetime(df_hourly)

        # log minimum and maximum datetime, total records
        logger.info(f"MIN DATETIME in {csv_file}: {min_datetime}")
        logger.info(f"MAX DATETIME in {csv_file}: {max_datetime}")
        logger.info(f"Total records in {csv_file}: {df_hourly.shape[0]}")

        # get metadata of the station
        station_metadata = get_metadata_from_filename(csv_file, metadata)
        station_id = station_metadata['stationid']

        df_hourly['datetime'] = df_hourly['datetime'].apply(lambda x: x.to_pydatetime())
        df_hourly.insert(1, 'time_key', df_hourly['datetime'].apply(lambda x: x.strftime('%Y%m%d%H')) )
        df_hourly.insert(2, 'station', int(station_id))
        df_hourly.insert(3, 'interval_length', 60)
        
        fieldnames = list(station_metadata['columnnames'].values())
        fieldnames.insert(1, 'time_key')
        fieldnames.insert(2, 'station')
        fieldnames.insert(3, 'interval_length')
        
        conn = connect_db(params_dict)        
        
        if station_metadata['filetype'].lower() == 'm':
            execute_batch_insertion(
                conn, 
                min_datetime, max_datetime, 
                df_hourly.round(4), fieldnames, met_tablename, 
                station_id,
                100
            )
        elif station_metadata['filetype'].lower() == 'a':
            execute_batch_insertion(
                conn,
                min_datetime, max_datetime,
                df_hourly.round(4), fieldnames, aqm_tablename,
                station_id,
                100
            )
        elif station_metadata['filetype'].lower() == 'ma':
            # split the combined dataframe
            df_hourly_met, df_hourly_aqm = split_met_aqm(df_hourly.round(4), met_columnnames, aqm_columnnames)
            # get min-max datetime
            min_datetime_met, max_datetime_met = get_min_max_datetime(df_hourly_met)
            min_datetime_aqm, max_datetime_aqm = get_min_max_datetime(df_hourly_aqm)
            
            execute_batch_insertion(
                conn,
                min_datetime_met, max_datetime_met,
                df_hourly_met.round(4), list(df_hourly_met.columns), met_tablename,
                station_id,
                100
            )
            execute_batch_insertion(
                conn,
                min_datetime_aqm, max_datetime_aqm,
                df_hourly_aqm.round(4), list(df_hourly_aqm.columns), aqm_tablename,
                station_id,
                100
            )            
        else:
            msg_obj.message = "Filetype Mismatched: Must be either M, A or MA."
            msg_obj.log_as_error        
        if conn:
            conn.close()

def lambda_handler(event, context):
    filename = event['Records'][0]['s3']['object']['key']
    bucket = event['Records'][0]['s3']['bucket']['name']
    
    csv_file = filename.split('/')[-1]
    file_path = '/tmp/{}'.format(csv_file)

    metadata_object = s3_client.get_object(Bucket=BUCKET_NAME, Key=METADATA_JSON_KEY)
    metadata = load_metadata(metadata_object)
    if metadata:
        try:
            s3_client.download_file(bucket, filename, file_path)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                msg_obj.message=f"File:{filename} does not exist in S3"
                msg_obj.log_as_error
                msg_obj.publish_to_sns
        else:
            load_to_db(file_path, metadata, 'dwer_met', 'dwer_aqm')
    else:
        msg_obj.message="Could not load metadata."
        msg_obj.log_as_error
        msg_obj.publish_to_sns