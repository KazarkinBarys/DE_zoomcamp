import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector




@task(log_prints=True, tags=["extract"], cache_key_fn=task_input_hash, cache_expiration=timedelta(days=100))
def download_data(csv_url: str):
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if csv_url.endswith('.csv.gz'):
        csv_name = f"taxi_zones.csv.gz"
    else:
        csv_name = f"taxi_zones.csv"
    os.system(f"wget {csv_url} -O {csv_name} -P C:\dataengeneering\zoomcamp\week_3")
    return(csv_name)

    
@task(log_prints = True)
def load_head(csv_name, table_name):
    df = pd.read_csv(csv_name ,nrows = 100)


    connection_block = SqlAlchemyConnector.load("postgres-connector")
    with connection_block.get_connection(begin=False) as engine:
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    
    return(df)




@task(log_prints=True)
def transform_data(csv_name,df,table_name):
    total_rows = 0
    total_bad_rows = 0
    total_rows_inserted= 0
    full_time = 0

    connection_block = SqlAlchemyConnector.load("postgres-connector")
    engine = connection_block.get_connection(begin=False) 
    
    
    df_iter = pd.read_csv(csv_name,iterator = True, chunksize = 100000)

    while len(df) > 0:
        try:
            start_time = time()

            df = next(df_iter)
            total_rows += len(df)
            total_r = len(df)
           
            
            #df = df[df['passenger_count'] != 0]
            
            total_bad_rows += (total_r - len(df))
            total_rows_inserted += len(df)

            df.to_sql(name=table_name, con=engine, if_exists='append')


            end_time = time()
            full_time += end_time - start_time
            print(f'total rows processed = {total_rows}\ntotal bad rows cleared = {total_bad_rows}\ntotal rows inserted = {total_rows_inserted}\ntook {full_time} seconds')
           

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break




   
    

@flow(name="Subflow", log_prints=True)
def log_subflow(table_name: str):
    print(f"Logging Subflow for: {table_name}")

@flow
def main_flow():


    table_name = f"taxi_zones"
    csv_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
        

    log_subflow(table_name)
    csv_name = download_data(csv_url)
    df  = load_head(csv_name,table_name)
    transform_data(csv_name,df,table_name)
    print("JOB`s Done!!!")

    

    

if __name__ == '__main__':

    
    main_flow()