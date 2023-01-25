import pandas as pd
import argparse
import os
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    #print("Downloading file")
    #csv_name = '/tmp/output.csv.gz'
    #os.system=(f"wget -v {url} -O {csv_name}")
    print(url)

    print("Creating engine")
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))

    df_iter = pd.read_csv(url, iterator=True, chunksize=100000, compression='gzip')

    for df in df_iter:
        print("Inserted another chunk")
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name=table_name, con=engine, if_exists='append')

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='db for postgres')
    parser.add_argument('--table_name', help='table_name for postgres')
    parser.add_argument('--url', help='URL for postgres')

    args = parser.parse_args()
    main(args)
