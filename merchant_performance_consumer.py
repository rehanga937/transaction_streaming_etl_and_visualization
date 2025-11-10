from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

import json
import os
import traceback

from confluent_kafka import Consumer
import clickhouse_connect
from clickhouse_connect.driver.exceptions import DatabaseError

from src import config
from src.helpers.clickhouse import insert_2_clickhouse


TABLE_NAME = 'merchant_perf'


# setup consumer
consumer_config = {
    "bootstrap.servers": config.KAKFA_BROKERS_FOR_LOCAL_MACHINE,
    "group.id": TABLE_NAME,
    "auto.offset.reset": "earliest" # https://quix.io/blog/kafka-auto-offset-reset-use-cases-and-pitfalls
}
consumer = Consumer(consumer_config)
consumer.subscribe([config.AGGREGATES_TOPIC])
print(f"\033[92mMerchant performance consumer subscribed to {config.AGGREGATES_TOPIC} topic.\033[00m")

# setup database
client = clickhouse_connect.get_client(host='localhost', port=os.getenv('CLICKHOUSE_PORT'), username=os.getenv('CLICKHOUSE_USER'), password=os.getenv('CLICKHOUSE_PASSWORD'))

try: 
    client.command(
        f"""
        CREATE TABLE {TABLE_NAME} (timestamp DateTime, merchantId UInt8, totalAmount UInt32, transactionCount UInt32) 
        ENGINE MergeTree 
        ORDER BY (merchantId, timestamp)
        """
    )
    print(f"\033[92mCreated new table {TABLE_NAME}\033[00m")
except DatabaseError: print(f"\033[94mTable {TABLE_NAME} already exists.\033[00m")


# write consumed messages to clickhouse
try:
    while True:
        msg = consumer.poll(1.0) # timeout is given in seconds, adding this to stop blocking for graceful keyboard interrupt handling
        if msg is None:
            continue
        if msg.error():
            print(f"\033[92mError: {msg.error()} \033[00m")
            continue

        value = msg.value().decode("utf-8")
        merchant_agg: dict = json.loads(value)
        timestamp = msg.timestamp()[1] # number of ms since epoch (UTC)

        # Write merchant_agg to a table in clickhouse 
        insert_2_clickhouse(client, merchant_agg, TABLE_NAME, timestamp)

        

except KeyboardInterrupt:
    print("\033[91mStopping merchant performance consumer...\033[00m")

except Exception as e:
    print(f"\033[91mError: {e}\033[00m")
    print(traceback.format_exc())

finally:
    client.close()
    consumer.close() # graceful shutdown - finish all the pending stuff