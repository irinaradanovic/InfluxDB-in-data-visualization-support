import os
import sys
import time
import pandas as pd
import numpy as np
from datetime import datetime
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS

def check_if_data_exists(url, token, org, bucket):
    print("Checking if data already exists in InfluxDB...")
    with InfluxDBClient(url=url, token=token, org=org) as client:
        query_api = client.query_api()
        
        flux_query = f'''
        from(bucket: "{bucket}")
          |> range(start: 2016-01-01T00:00:00Z, stop: now())
          |> filter(fn: (r) => r["_measurement"] == "smart_home_metrics")
          |> limit(n: 1)
        '''
        try:
            result = query_api.query(flux_query)
            if len(result) > 0:
                return True
        except Exception as e:
            print(f"Note: Could not query DB or bucket empty (Details: {e})")
    return False


def wait_for_influx(url, timeout=60):
    start_time = time.time()
    print("Waiting for InfluxDB...")
    while time.time() - start_time < timeout:
        try:
            with InfluxDBClient(url=url, token="dummy") as client:
                client.ping()
            print("InfluxDB is available")
            return True
        except Exception:
            time.sleep(2)
    print("Error: InfluxDB is not available within the specified time frame.")
    return False

def clean_and_prepare_data(file_path):
    print("Starting to load and clean the CSV file...")
    if not os.path.exists(file_path):
        print(f"Error: Dataset not found at location {file_path}")
        sys.exit(1)
        
    # Load CSV file
    df = pd.read_csv(file_path)
    
    # Removing the last row which is invalid
    df = df[0:-1]
    
    # Remove ' [kW]' suffix from column names
    df.columns = [col.replace(' [kW]', '') for col in df.columns]
    
    # --- AGREGATION ---
    df['Furnace'] = df[['Furnace 1', 'Furnace 2']].sum(axis=1)

    df['Kitchen'] = df[['Kitchen 12', 'Kitchen 14', 'Kitchen 38']].mean(axis=1)
    
    df = df.drop(['Furnace 1', 'Furnace 2', 'Kitchen 12', 'Kitchen 14', 'Kitchen 38'], axis=1)

    df['cloudCover'] = pd.to_numeric(df['cloudCover'], errors='coerce')
    df['cloudCover'] = df['cloudCover'].bfill()

    df = df.drop(columns=['House overall', 'Solar']) # we will use columns 'use' and 'gen' instead
    df = df.drop(columns=['icon']) # we will use 'summary' instead of 'icon'
    
    # Reconstruct time column as a proper datetime index
    time_index = pd.date_range('2016-01-01 05:00', periods=len(df), freq='min')
    df['timestamp'] = pd.DatetimeIndex(time_index)
    df = df.drop(['time'], axis=1)
    
    print(f"Cleaning completed. Prepared {len(df)} rows for insertion.")
    return df

def main():
    # Loading configuration
    url = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
    token = os.getenv("INFLUXDB_TOKEN")
    org = os.getenv("INFLUXDB_ORG", "my_org")
    bucket = os.getenv("INFLUXDB_BUCKET", "smart_home")
    csv_path = "/app/data/HomeC.csv"

    if not wait_for_influx(url):
        sys.exit(1)

    if check_if_data_exists(url, token, org, bucket):
        print("=== INFO: Data already exists in InfluxDB. Skipping ingest! ===")
        sys.exit(0)  # Turn off container

    # Cleaning data
    df = clean_and_prepare_data(csv_path)

    # Initializing InfluxDB client
    print(f"Connecting to InfluxDB ({url}), Bucket: {bucket}...")
    
    # Using WriteOptions for optimized sending in batches (chunks)
    with InfluxDBClient(url=url, token=token, org=org) as client:
        with client.write_api(write_options=WriteOptions(
            batch_size=5000, 
            flush_interval=1_000,
            jitter_interval=0,
            retry_interval=5_000,
            max_retries=5
        )) as write_api:
            
            points = []
            print("Converting data to InfluxDB Points format and sending in batches...")
            
            for index, row in df.iterrows():
                point = Point("smart_home_metrics").time(row['timestamp'])
                if 'summary' in df.columns and not pd.isna(row['summary']):
                    point.tag("weather_summary", str(row['summary']))
                
                for col in df.columns:
                    if col not in ['timestamp', 'summary']:
                        if not pd.isna(row[col]):
                            point.field(col, float(row[col]))
                
                points.append(point)

                if len(points) % 50000 == 0:
                    write_api.write(bucket=bucket, org=org, record=points)
                    print(f"Inserted {len(points)} out of {len(df)} rows...")
                    points = [] # Clearing memory
            
            # Sending remaining data that didn't fill the last full batch
            if points:
                write_api.write(bucket=bucket, org=org, record=points)
                
    print("All data has been successfully imported into InfluxDB!")

if __name__ == "__main__":
    main()