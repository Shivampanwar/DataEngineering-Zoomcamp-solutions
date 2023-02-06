from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(log_prints=True, tags=["fetc"], cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url):
    print ("fetching dataset from "+str(dataset_url))

    df = pd.read_csv(dataset_url)
    print ("Dataset shape is {}".format(str(df.shape[0])))
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@flow()
def etl_web_to_gcs_hw() -> None:
    """The main ETL function"""
    color = "green"

    year = 2020

    month = 1
    # dataset_file = f"{color}_tripdata_{year}-{month:02}"
    # dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    dataset_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-01.csv.gz"
    df = fetch(dataset_url)
    print ("Fetching is done")
    df  = clean(df)
    print (df.shape)


if __name__ == "__main__":
    etl_web_to_gcs_hw()