import logging
from deltalake import DeltaTable, write_deltalake
import sys
import boto3
from datetime import datetime
import polars as pl


# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(message)s"
# )
logger = logging.getLogger(__name__)
logger.info("started working on scd2")

# Checks if there historical data in scd2 bucket
def check_scd2_table_empty(storage_config):
    bucket_name = "scd2"
    client = boto3.client(
        "s3",
        endpoint_url=storage_config["AWS_ENDPOINT_URL"],
        aws_access_key_id=storage_config["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=storage_config["AWS_SECRET_ACCESS_KEY"]
    )

    objects = client.list_objects(
        Bucket = bucket_name, 
    )

    if "Contents" in objects.keys():
        return True
    
    return False

# Initializes historical values in case the bucket is empty, set valid_from=1970 and valid_to=9999
# This runs only once
def initialize_scd2_table(cdc_table, s3_path, storage_config):
    cdc_table = cdc_table.with_columns(
        pl.date(1970, 1, 1).alias("valid_from_date"), 
        pl.date(9999, 12, 31).alias("valid_to_date")
    )

    # write to delta
    cdc_table.sink_delta(
        s3_path,
        storage_options = storage_config,
        mode = "append"
    )

# This function reads the table that stores the changes coming from pg
def read_delta_table(s3_path, storage_config):
    logger.info(f"Reading {s3_path} delta table!")
    df = pl.scan_delta(s3_path, storage_options = storage_config)

    return df

# Upserting data based on last batch that we received with cdc
def apply_scd2_on_last_batch(cdc_table, s3_path_scd, storage_config):
    cdc_table = cdc_table.filter(
        pl.col("updated_at") == datetime(2026, 5, 2) 
    )
    cdc_table = cdc_table.with_columns(
        pl.date(1970, 1, 1).alias("valid_from_date"), 
        pl.date(9999, 12, 31).alias("valid_to_date")
    )
    cdc_table = cdc_table.collect().to_arrow()    

    target_dt = DeltaTable(s3_path_scd, storage_options=storage_config)

    # Close records based the update_date of the source, and update values
    (
        target_dt.merge(
            source = cdc_table,
            predicate = "s.customer_id = t.customer_id",
            source_alias = "s",
            target_alias = "t"
        )
        .when_matched_update(
            updates = {
                "valid_to_date": "s.updated_at",
                "first_name": "s.first_name"
                },
            predicate = "t.valid_to_date = DATE '9999-12-31 00:00:00'"
        )
        .execute()
    )

    # Append rows to the delta table whether they match or not, It doesn't matter
    write_deltalake(
        s3_path_scd,
        data = cdc_table,
        storage_options = storage_config,
        mode = "append",
    )

def load_config():
    from pathlib import Path
    sys.path.append(str(Path(__file__).resolve().parent.parent))
    from config import S3_PATH_CDC, S3_PATH_SCD2, STORAGE_CONFIG

    return S3_PATH_CDC, S3_PATH_SCD2, STORAGE_CONFIG

if __name__ == "__main__":
    S3_PATH_CDC, S3_PATH_SCD2, STORAGE_CONFIG = load_config()
    cdc_table = read_delta_table(S3_PATH_CDC, STORAGE_CONFIG)

    if not check_scd2_table_empty(STORAGE_CONFIG):
        initialize_scd2_table(cdc_table, S3_PATH_SCD2, STORAGE_CONFIG)
        sys.exit(0)

    apply_scd2_on_last_batch(cdc_table, S3_PATH_SCD2, STORAGE_CONFIG)

    scd2_table = read_delta_table(S3_PATH_SCD2, STORAGE_CONFIG)
    scd2_table.show(limit = 20)


