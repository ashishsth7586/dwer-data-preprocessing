import os
import json
import time
import logging
from datetime import datetime

import botocore
import boto3
import psycopg2
import pandas as pd

from functools import wraps
from memory_profiler import memory_usage

filepath_metadata = os.environ['FILEPATH_METADATA']
SAMPLING_FREQUENCY = os.environ['SAMPLING_FREQUENCY']
met_columnnames = ['datetime', 'sp04', 'sp10', 'gu04', 'gu10', 'dn04', 'dn10', 'sg04', 'sg10', 'at02', 'at10', 'delt', 'rh02', 'rh10', 'pres', 'rain', 'swin', 'swout', 'iwin', 'iwout', 'grad', 'nrad']
aqm_columnnames = ['datetime', 'pm2_5', 'pm10', 'pmcrs', 'so2', 'co', 'no', 'no2', 'nox', 'o3']
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

s3_client = boto3.client('s3')

# Get values from Environment variables
DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ['DB_PORT']
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']

# Logging Configurations
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def profile(fn):
    @wraps(fn)
    def inner(*args, **kwargs):
        fn_kwargs_str = ', '.join(f'{k}={v}' for k, v in kwargs.items())
        
        logger.debug(f'\n{fn.__name__}({fn_kwargs_str})')

        # Measure time
        t = time.perf_counter()
        retval = fn(*args, **kwargs)
        elapsed = time.perf_counter() - t
        
        logger.debug(f'TIME: {elapsed:0.4}')

        # # Measure memory
        # mem, retval = memory_usage((fn, args, kwargs), retval=True, timeout=200, interval=1e-7)
        
        # logger.debug(f'MEMORY: {max(mem) - min(mem)}')
        
        return retval

    return inner

def load_metadata(filepath):
    try:
        with open(filepath) as f:
            data = json.load(f)
        return data
    except IOError:
        logger.error("An error occurred. No such file or directory.")

def get_translation_from_filename(filename, metadata):
    
    if isinstance(metadata, dict):
        translations = metadata['translations']

        for i in range(len(translations)):
            if translations[i]['filename'].lower() == filename.lower():
                return translations[i]['columnnames']
    else:
        logger.error("Metadata is not Dictionary Object.")

def get_metadata_from_filename(filename, metadata):
    if isinstance(metadata, dict):
        translations = metadata['translations']

        for i in range(len(translations)):
            if translations[i]['filename'].lower() == filename.lower():
                return translations[i]
    else:
        logger.error("Metadata is not Dictionary Object.")

def get_records_from_dataframe(df):
    records = []
    for index, row in df.iterrows():
        records.append(row)
    return records

def get_conn_instance_postgres(host, port, database, user, password):
    try:
        conn = psycopg2.connect(host=host, port=port, database=database, user=user, password=password)
    except psycopg2.OperationalError as err:
        logger.error(err)
        conn = None
    return conn

def insert_to_db(host, port, database, user, password, fieldnames, data, tablename):
    import datetime

    conn = get_conn_instance_postgres(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD)

    if conn is not None:
        try:
            cur = conn.cursor()
            query = 'INSERT INTO ' + 'core_data.' + tablename + '(' + ','.join(fieldname for fieldname in fieldnames) + ') VALUES(' + ','.join(['%s'] * len(fieldnames)) + ');'
        
            record = []

            record.append(data['datetime'])
            record.append(data['station'])
            record.append(data['interval_length'])

            met_data = list(data['weather_data'].values())

            for cellvalue in met_data:
                record.append(cellvalue)
            try:
                cur.execute(query, record)
                conn.commit()
            except Exception as err:
                logger.error(err)
                conn.rollback()

        except (Exception, psycopg2.Error) as error:
            logger.error(error)
        finally:
            if cur:
                cur.close()
                conn.close()

def split_dataframe_met_aqm(df, met_columnnames, aqm_columnnames):
    all_columnnames_in_df = list(df.columns)

    met_columns = []
    aqm_columns = []

    for columnname in all_columnnames_in_df:
        if columnname in met_columnnames:
            met_columns.append(columnname)
        if columnname in aqm_columnnames:
            aqm_columns.append(columnname)
    return df[met_columns], df[aqm_columns]

def predelete_records_from_db(host, port, database, user, password, min_datetime, max_datetime, tablename, station_id):

    conn = get_conn_instance_postgres(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD)

    if conn is not None:
        try:
            cur = conn.cursor()
            query = "DELETE FROM " + "core_data." + tablename + " WHERE station=" + station_id + " AND datetime>=" + "'" + min_datetime + "'" + " AND datetime<=" + "'" + max_datetime + "'"
            cur.execute(query)
            conn.commit()

            count = cur.rowcount
            logger.info(f"{count} records deleted successfully.")

        except (Exception, psycopg2.Error) as error:
            logger.error(error)
        finally:
            if cur:
                cur.close()
                conn.close()
    else:
        logger.debug("Database connection failed!")

def get_max_datetime(df):
    return df.index.max()

def get_min_datetime(df):
    return df.index.min()

metadata = load_metadata(filepath_metadata)

@profile
def load_to_postgres(filename, metadata, met_tablename='dwer_met', aqm_tablename='dwer_aqm'):

    if filename:
        splitted_filenames = filename.split('_')
        date, extension = splitted_filenames[-1].split('.')
        filename_without_date = '_'.join(splitted_filenames[0:-1]) + '.' + extension

        csv_file = filename_without_date.split('/')[-1]

        columnnames_tx = get_translation_from_filename(csv_file, metadata)

        df = pd.read_csv(filename)
        
        if 'Date_Time.1' in list(df.columns):
            df = df.drop('Date_Time.1', axis=1)

        df.rename(columns=columnnames_tx, inplace=True)

        list_of_columnnames_in_db = list(columnnames_tx.values())
        renamed_columnnames = list(df.columns)

        for value in renamed_columnnames:
            if value not in list_of_columnnames_in_db:
                logger.info(f"Dropping {value} column.")
                df = df.drop(value, axis=1)
                logger.info(f"Successfully dropped {value} column.")
        
        df = df.loc[:, ~df.columns.duplicated()]

        col_agg_mappings = { key: column_agg_mapping[key] for key in list(columnnames_tx.values())[1:]}

        data_series = pd.notnull(df["datetime"])
        df['datetime'] = df['datetime'][data_series].apply(lambda x: datetime.strptime(x, '%d%m%Y %H%M'))
        indexes = pd.DatetimeIndex(df['datetime'])
        df = df.set_index(indexes)

        df_hourly = df.resample(SAMPLING_FREQUENCY).agg(col_agg_mappings)

        df_hourly['datetime'] = df_hourly.index

        df_hourly = df_hourly.where(pd.notnull(df_hourly), None)
        
        max_datetime = get_max_datetime(df_hourly)
        min_datetime = get_min_datetime(df_hourly)

        logger.info(f"MAX DATETIME in {csv_file}: {max_datetime}")
        logger.info(f"MIN DATETIME in {csv_file}: {min_datetime}")
        logger.info(f"Total records in {csv_file}: {df_hourly.shape[0]}")
        station_metadata = get_metadata_from_filename(csv_file, metadata)

        if station_metadata['filetype'].lower() == 'm':
            logger.debug("File consists of only Meteorological data.")
            station_id = station_metadata['stationid']

            predelete_records_from_db(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, str(min_datetime), str(max_datetime), met_tablename, str(station_id))

            all_records = get_records_from_dataframe(df_hourly.round(4))

            for index in range(len(all_records)):
                each_records = dict(all_records[index])

                data = {
                    'datetime': each_records['datetime'].to_pydatetime(),
                    'station': int(station_id),
                    'interval_length': 60,
                    'weather_data': each_records
                }

                data['weather_data'].pop('datetime')

                fieldnames = list(station_metadata['columnnames'].values())
                fieldnames.insert(1, 'station')
                fieldnames.insert(2, 'interval_length')

                insert_to_db(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, fieldnames, data, met_tablename)
                    
        elif station_metadata['filetype'].lower() == 'a':
            logger.debug(f"File consists of only Air Quality data.")

            station_id = station_metadata['stationid']

            predelete_records_from_db(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, str(min_datetime), str(max_datetime), aqm_tablename, str(station_id))

            all_records = get_records_from_dataframe(df_hourly.round(4))

            for index in range(len(all_records)):
                each_records = dict(all_records[index])

                data = {
                    'datetime': each_records['datetime'].to_pydatetime(),
                    'station': int(station_id),
                    'interval_length': 60,
                    'weather_data': each_records
                }

                data['weather_data'].pop('datetime')

                fieldnames = list(station_metadata['columnnames'].values())
                fieldnames.insert(1, 'station')
                fieldnames.insert(2, 'interval_length')

                insert_to_db(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, fieldnames, data, aqm_tablename)
                    
        elif station_metadata['filetype'].lower() == 'ma':
            logger.debug('File consists of both Meteorological and Air Quality Data')

            df_hourly_met, df_hourly_aqm = split_dataframe_met_aqm(df_hourly.round(4), met_columnnames, aqm_columnnames)

            df_hourly_met['datetime'] = df_hourly_met.index
            df_hourly_aqm['datetime'] = df_hourly_aqm.index

            max_datetime_met = get_max_datetime(df_hourly_met)
            min_datetime_met = get_min_datetime(df_hourly_met)

            max_datetime_aqm = get_max_datetime(df_hourly_aqm)
            min_datetime_aqm = get_min_datetime(df_hourly_aqm)

            station_id = station_metadata['stationid']

            predelete_records_from_db(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, str(min_datetime_met), str(max_datetime_met), met_tablename, str(station_id))
            predelete_records_from_db(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, str(min_datetime_aqm), str(max_datetime_aqm), aqm_tablename, str(station_id))

            hourly_met_records = get_records_from_dataframe(df_hourly_met)
            hourly_aqm_records = get_records_from_dataframe(df_hourly_aqm)

            for index in range(len(hourly_met_records)):
                each_records = dict(hourly_met_records[index])
            
                data = {
                    'datetime': each_records['datetime'].to_pydatetime(),
                    'station': int(station_id),
                    'interval_length': 60,
                    'weather_data': each_records
                }
                data['weather_data'].pop('datetime')

                all_met_columns = list(df_hourly_met.columns)
                all_met_columns.remove('datetime')

                fieldnames = all_met_columns
                fieldnames.insert(0, 'datetime')
                fieldnames.insert(1, 'station')
                fieldnames.insert(2, 'interval_length')

                insert_to_db(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, fieldnames, data, met_tablename)
            
            for index in range(len(hourly_aqm_records)):
                each_records = dict(hourly_aqm_records[index])
            
                data = {
                    'datetime': each_records['datetime'].to_pydatetime(),
                    'station': int(station_id),
                    'interval_length': 60,
                    'weather_data': each_records
                }
                data['weather_data'].pop('datetime')

                all_aqm_columns = list(df_hourly_aqm.columns)
                all_aqm_columns.remove('datetime')

                fieldnames = all_aqm_columns
                fieldnames.insert(0, 'datetime')
                fieldnames.insert(1, 'station')
                fieldnames.insert(2, 'interval_length')

                insert_to_db(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, fieldnames, data, aqm_tablename)
        else:
            logger.error(f"{csv_file} filetype Mismatched!")
    else:
        logger.warning(f"{filename} not found!")


def lambda_handler(event, context):
    
    filename = event['Records'][0]['s3']['object']['key']
    bucket = event['Records'][0]['s3']['bucket']['name']

    tmpkey = filename.replace('/', '')

    file_path = '/tmp/{}'.format(tmpkey)
    
    try:
        s3_client.download_file(bucket, filename, file_path)
        logger.debug(f"Successfully downloaded {filename} from s3")

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            logger.error("The object does not exist.")

    logger.debug(f"STARTED: Loading {filename} data")

    load_to_postgres(file_path, metadata, 'dwer_met', 'dwer_aqm')

    logger.info(f"Completed!")