BOOTSTRAP_SERVER = 'kafka:29092'
CDC_TOPIC = 'pg-changes.public.customers'
CONSUMER_GROUP = "consumer-group-clean-v2"
STORAGE_CONFIG = {
    "AWS_ACCESS_KEY_ID": "admin",
    "AWS_SECRET_ACCESS_KEY": "admin123",
    "AWS_ENDPOINT_URL": "http://172.18.0.2:9000",
    'AWS_REGION': 'us-east-1',
    'allow_http': 'true'
}
S3_PATH_CDC = "s3://cdc-data/updated_records"
S3_PATH_SCD2 = "s3://scd2/updated_records"