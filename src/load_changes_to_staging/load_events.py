from confluent_kafka import Consumer
import json
from deltalake import write_deltalake, DeltaTable
from deltalake.exceptions import TableNotFoundError
import pandas as pd
import time
import logging
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import S3_PATH_CDC, STORAGE_CONFIG, BOOTSTRAP_SERVER, CDC_TOPIC, CONSUMER_GROUP


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)

def load_cdc_events_to_delta(batch_size = 5):
    logger.info(f"boostrap server: {BOOTSTRAP_SERVER}")
    logger.info(f"changes topic: {CDC_TOPIC}")

    changes_counter = 0
    logger.info(f"set changes counter to 0")

    consumer_configuration = {
        'bootstrap.servers': BOOTSTRAP_SERVER,
        'client.id': 'consumer1',
        'group.id': CONSUMER_GROUP,
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(consumer_configuration)
    logger.info(f"Subscribed to {CDC_TOPIC}")

    consumer.subscribe([CDC_TOPIC])
    logger.info("Subscribed to demo_topic")

    current_batch = pd.DataFrame()

    while True:
        msg = consumer.poll(1.0)
        time.sleep(2)
        if msg is None:
            continue
        
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        
        message = json.loads(msg.value().decode("utf-8"))
        logger.info("decoded event")
        logger.info(f"Received message from topic => {msg.topic()}, \
            partition => {msg.partition()}")
    
        customer_record = parse_event_to_customer_record(message)
        logger.info(f"message: {customer_record}")

        customer_record["updated_at"] = pd.to_datetime(customer_record["created_at"], unit = "us").date()

        logger.info("concatenated customer_record with current_batch")
        current_batch = concat_record_with_currrent_batch(current_batch, customer_record)
        changes_counter += 1

        if changes_counter == batch_size:
            write_dataframe_to_delta_table(STORAGE_CONFIG, S3_PATH_CDC, current_batch)
            current_batch = pd.DataFrame()
            changes_counter = 0

    consumer.close()
    logger.info("Connection closed unexpectedly")

def parse_event_to_customer_record(message):
    customer_record = {}
    new_record_json = message["payload"]["after"]

    customer_record["customer_id"] = new_record_json["customer_id"]
    customer_record["first_name"] = new_record_json["first_name"]
    customer_record["last_name"] = new_record_json["last_name"]
    customer_record["email"] = new_record_json["email"]
    customer_record["street"] = new_record_json["street"]
    customer_record["city"] = new_record_json["city"]
    customer_record["country"] = new_record_json["country"]
    customer_record["created_at"] = new_record_json["created_at"]

    return customer_record

def concat_record_with_currrent_batch(current_batch, record):
    record = pd.DataFrame([record])
    return pd.concat([current_batch, record], ignore_index = True)

def write_dataframe_to_delta_table(storage_config, s3_path, df):
    try:
        dt = DeltaTable(s3_path, storage_options = storage_config)
        logger.info(f"table {s3_path} exist, setting write mode to append")
        mode = 'append'
    
    except:
        logger.info(f"table {s3_path} doesn't exist, setting write mode to overwrite")
        mode = 'overwrite'

    logger.info(f"Output mode is {mode}")
    write_deltalake(
        s3_path,
        data = df,
        storage_options = storage_config,
        mode = mode,
        partition_by = ["updated_at"]
    )

if __name__ == '__main__':
    load_cdc_events_to_delta()

