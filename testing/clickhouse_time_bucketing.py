from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

import os

import clickhouse_connect
from clickhouse_connect.driver.exceptions import DatabaseError


TABLE_NAME = 'merchant_perf'

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

time_bucket_query = f"""
SELECT
    merchantId,
    toStartOfInterval(timestamp, INTERVAL 1 MINUTE) AS window_start,
    sum(totalAmount) AS total_amount
FROM merchant_perf
WHERE timestamp >= now() - INTERVAL 60 MINUTE
GROUP BY window_start, merchantId
ORDER BY window_start
"""

result = client.query(time_bucket_query)
print(result.result_rows)

