import os
import json
import time
import logging
from datetime import datetime

import boto3
import psycopg2
import pandas as pd


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

db_host = os.environ['DB_HOST']
db_port = os.environ['DB_PORT']
db_name = os.environ['DB_NAME']
db_user = os.environ['DB_USER']
db_password = os.environ['DB_PASSWORD']


logger = logging.getLogger()
logger.setLevel(logging.INFO)

def load_metadata(filepath):
    with open(filepath) as f:
        data = json.load(f)
    return data

def get_translation_from_filename(filename, metadata):
    translations = metadata['translations']

    for i in range(len(translations)):
        if translations[i]['filename'].lower() == filename.lower():
            return translations[i]['columnnames']

def get_metadata_from_filename(filename, metadata):
    translations = metadata['translations']

    for i in range(len(translations)):
        if translations[i]['filename'].lower() == filename.lower():
            return translations[i]

def get_records_from_dataframe(df):
    records = []
    for index, row in df.iterrows():
        records.append(row)
    return records

def get_conn_instance_postgres(host, port, database, user, password):
    conn = psycopg2.connect(host=host, port=port, database=database, user=user, password=password)
    return conn

def insert_to_db(host, port, database, user, password, fieldnames, data, tablename):
    import datetime

    conn = get_conn_instance_postgres(db_host, db_port, db_name, db_user, db_password)

    if data:
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
            
            cur.execute(query, record)
            conn.commit()
        except (Exception, psycopg2.Error) as error:
            print(f"Failed Inserting {tablename} record: {error}")
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

def predelete_records_from_db(host, port, database, user, password, min_datetime, max_datetime, tablename, station_id) -> None:
    conn = get_conn_instance_postgres(db_host, db_port, db_name, db_user, db_password)

    try:
        cur = conn.cursor()
        query = "DELETE FROM " + "core_data." + tablename + " WHERE station=" + station_id + " AND datetime>=" + "'" + min_datetime + "'" + " AND datetime<=" + "'" + max_datetime + "'"
        cur.execute(query)
        conn.commit()

        count = cur.rowcount
        print(count, "records deleted successfully.")
    except (Exception, psycopg2.Error) as error:
        print(f"Failed Inserting {tablename} record: {error}")
    finally:
        if cur:
            cur.close()
            conn.close()

def get_max_datetime(df):
    return df.index.max()

def get_min_datetime(df):
    return df.index.min()

metadata = load_metadata(filepath_metadata)

def load_to_postgres(filename, metadata, met_tablename='dwer_met', aqm_tablename='dwer_aqm'):
    if filename:
        print(f"====={filename}=====")
        
        splitted_filenames = filename.split('_')
        date, extension = splitted_filenames[-1].split('.')
        filename_without_date = '_'.join(splitted_filenames[0:-1]) + '.' + extension

        # print(f"====={filename_without_date}=====")
        csv_file = filename_without_date.split('/')[-1]
        # print(f"====={csv_file}=====")

        logger.info(f"FINAL FILE: {csv_file}")
        columnnames_tx = get_translation_from_filename(csv_file, metadata)
        # filepath = base_dir + filename

        df = pd.read_csv(filename)
        
        if 'Date_Time.1' in list(df.columns):
            df = df.drop('Date_Time.1', axis=1)

        df.rename(columns=columnnames_tx, inplace=True)
        
        list_of_columnnames_in_db = list(columnnames_tx.values())
        renamed_columnnames = list(df.columns)

        for value in renamed_columnnames:
            if value not in list_of_columnnames_in_db:
                print(f"Dropping {value} column.")
                df = df.drop(value, axis=1)
                print(f"Successfully dropped {value} column.")
        
        df = df.loc[:, ~df.columns.duplicated()]

        col_agg_mappings = { key: column_agg_mapping[key] for key in list(columnnames_tx.values())[1:]}

        data_series = pd.notnull(df["datetime"])
        df['datetime'] = df['datetime'][data_series].apply(lambda x: datetime.strptime(x, '%d%m%Y %H%M'))
        indexes = pd.DatetimeIndex(df['datetime'])
        df = df.set_index(indexes)

        df_hourly = df.resample(SAMPLING_FREQUENCY).agg(col_agg_mappings)

        df_hourly['datetime'] = df_hourly.index

        max_datetime = get_max_datetime(df_hourly)
        min_datetime = get_min_datetime(df_hourly)

        station_metadata = get_metadata_from_filename(csv_file, metadata)

        if station_metadata['filetype'].lower() == 'm':
            print("File consists of only Meteorological data.")
            station_id = station_metadata['stationid']

            predelete_records_from_db(db_host, db_port, db_name, db_user, db_password, str(min_datetime), str(max_datetime), met_tablename, str(station_id))

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

                insert_to_db(db_host, db_port, db_name, db_user, db_password, fieldnames, data, met_tablename)
            print(f"Done uploading all the records in {filename}")
        
        elif station_metadata['filetype'].lower() == 'a':
            print(f"File consists of only Air Quality data.")

            station_id = station_metadata['stationid']

            predelete_records_from_db(db_host, db_port, db_name, db_user, db_password, str(min_datetime), str(max_datetime), aqm_tablename, str(station_id))

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

                insert_to_db(db_host, db_port, db_name, db_user, db_password, fieldnames, data, aqm_tablename)
            print(f"Done uploading all the records in {filename}")
        
        elif station_metadata['filetype'].lower() == 'ma':
            print('File consists of both Meteorological and Air Quality Data')

            df_hourly_met, df_hourly_aqm = split_dataframe_met_aqm(df_hourly.round(4), met_columnnames, aqm_columnnames)

            df_hourly_met['datetime'] = df_hourly_met.index
            df_hourly_aqm['datetime'] = df_hourly_aqm.index

            max_datetime_met = get_max_datetime(df_hourly_met)
            min_datetime_met = get_min_datetime(df_hourly_met)

            max_datetime_aqm = get_max_datetime(df_hourly_aqm)
            min_datetime_aqm = get_min_datetime(df_hourly_aqm)

            station_id = station_metadata['stationid']

            predelete_records_from_db(db_host, db_port, db_name, db_user, db_password, str(min_datetime_met), str(max_datetime_met), met_tablename, str(station_id))
            predelete_records_from_db(db_host, db_port, db_name, db_user, db_password, str(min_datetime_aqm), str(max_datetime_aqm), aqm_tablename, str(station_id))

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

                insert_to_db(db_host, db_port, db_name, db_user, db_password, fieldnames, data, met_tablename)
            
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

                insert_to_db(db_host, db_port, db_name, db_user, db_password, fieldnames, data, aqm_tablename)

            print(f"Done. Uploading all the records in {filename}")
        
        else:
            print(f"{filename}: Filetype Mismatched!")
    else:
        print(f"{filename} not found!")


def lambda_handler(event, context):
    
    filename = event['Records'][0]['s3']['object']['key']
    bucket = event['Records'][0]['s3']['bucket']['name']
    logger.info(f"EVENTS: {event}")

    tmpkey = filename.replace('/', '')

    file_path = '/tmp/{}'.format(tmpkey)
    logger.info(f"FILEPATH: {tmpkey}")

    s3_client.download_file(bucket, filename, file_path)

    logger.info(f"STARTED: Loading {filename} data->")

    load_to_postgres(file_path, metadata, 'dwer_met', 'dwer_aqm')

    logger.info(f"SUCCESS: Completed uploading the files.")    
    
    return {
        "status": "Successfully Inserted all the files to POSTGRES!!"
    }