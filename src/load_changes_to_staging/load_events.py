from confluent_kafka import Consumer
from datetime import datetime
import json
from deltalake import write_deltalake
import pandas as pd
import time
import logging

BOOTSTRAP_SERVER = 'kafka:29092'
CDC_TOPIC = 'pg-changes.public.customers'
CONSUMER_GROUP = "consumer-group-clean-v2"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)

def load_cdc_events_to_delta():
    logger.info(f"boostrap server: {BOOTSTRAP_SERVER}")
    logger.info(f"changes topic: {CDC_TOPIC}")

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


if __name__ == '__main__':
    load_cdc_events_to_delta()

