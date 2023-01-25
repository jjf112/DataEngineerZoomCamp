import pandas as pd
from sqlalchemy import create_engine


dfit = pd.read_csv('https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz', iterator=True, chunksize=100000, compression='gzip')
dfit2 = pd.read_csv('https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv', iterator=True, chunksize=100000)
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

for df in dfit:
    print("Running batch")
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    df.to_sql(name='green_tripdata_201901', con=engine, if_exists='append')

for df in dfit2:
    print("Running batch 2")
    df.to_sql(name='taxi_zones', con=engine, if_exists='append')

