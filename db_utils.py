import yaml
import os
import time
import pandas as pd
import psycopg2 
import streamlit as st 
import snowflake.connector  
import boto3
import json
import warnings
warnings.filterwarnings('ignore')
import datetime 

from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

def get_sf_connection(user_email):
    if True:
        account="PORTER-INDIA"
        conn = snowflake.connector.connect(
            user=user_email,
            account=account,
            authenticator="externalbrowser",
            warehouse= "WH_PROD_SUPPO_COMM_XS",
            database = 'PROD_CURATED',
            schema = 'SUPPORT_COMMUNICATION'
        )
    return conn  


def fetch_data(conn,query):
    query_start_ts = time.time()
    no_result = True
    try:
        df = pd.read_sql_query(query, conn)
    except Exception as e:
        log('There was an unexpected error of type {}'.format(e)) 
    seconds_taken = time.time() - query_start_ts
    log('Query run time in seconds : {}'.format(seconds_taken))
    df.columns = [col.lower() for col in df.columns] 
    return df


time_now = lambda : datetime.datetime.now().strftime('%Y:%m:%d %H:%M:%S')
def log(comment):
    print(f"{time_now()}: {comment}")

def write_dataframe_to_snowflake(table_name,
                                 dataframe,
                                 database='DEV_ELDORIA',
                                 schema='SANDBOX',
                                 user='murtaza.jalal@theporter.in'):
    """
    Writes a DataFrame to Snowflake using SSO (external browser auth).
    """
    engine_url = URL(
        user=user,
        account='PORTER-INDIA',
        warehouse='WH_PROD_SUPPO_COMM_XS',
        role='ROLE_SUPPO_COMM', 
        database=database,
        schema=schema,
        authenticator='externalbrowser'
    )

    engine = create_engine(engine_url)

    with engine.connect() as connection:
        # Write the DataFrame to the SQL table
        log(f"Writing DataFrame to table: {table_name}")
        dataframe.to_sql(
            table_name,
            con=connection,
            schema=schema,
            index=False,
            chunksize=100000,
            if_exists='replace',
            method='multi'
        )

    log(f"Table '{table_name}' written to Snowflake")


def append_dataframe_to_snowflake(table_name,
                                  dataframe,
                                  database='DEV_ELDORIA',
                                  schema='SANDBOX',
                                  user='murtaza.jalal@theporter.in'):
    """
    Appends a DataFrame to an existing table in Snowflake using SSO (external browser auth).
    If the table does not exist, it will be created.
    """
    engine_url = URL(
        user=user,
        account='PORTER-INDIA',
        warehouse='WH_PROD_SUPPO_COMM_XS',
        role='ROLE_SUPPO_COMM',
        database=database,
        schema=schema,
        authenticator='externalbrowser'
    )

    engine = create_engine(engine_url)

    try:
        with engine.connect() as connection:
            # Write the DataFrame to the SQL table, appending if exists
            log(f"Appending DataFrame to table: {table_name} (if_exists='append')")
            dataframe.to_sql(
                table_name,
                con=connection,
                schema=schema,
                index=False,
                chunksize=100000,
                if_exists='append', # Key difference: 'append'
                method='multi'
            )
        log(f"Table '{table_name}' appended to Snowflake.")
    except Exception as e:
        log(f"Error appending DataFrame to Snowflake: {e}")
        raise