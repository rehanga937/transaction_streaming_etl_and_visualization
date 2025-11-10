# https://clickhouse.com/docs/integrations/python

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

import os

import clickhouse_connect
from clickhouse_connect.driver.exceptions import DatabaseError


client = clickhouse_connect.get_client(host='localhost', port=os.getenv('CLICKHOUSE_PORT'), username=os.getenv('CLICKHOUSE_USER'), password=os.getenv('CLICKHOUSE_PASSWORD'))

try: 
    client.command('CREATE TABLE new_table (key UInt32, value String, metric Float64) ENGINE MergeTree ORDER BY key')
    print("\033[92mCreated new table new_table\033[00m")
except DatabaseError: print("\033[94mTable already exists.\033[00m")

row1 = [1000, 'String Value 1000', 5.233]
row2 = [2000, 'String Value 2000', -107.04]
data = [row1, row2]
client.insert('new_table', data, column_names=['key', 'value', 'metric'])

result = client.query('SELECT max(key), avg(metric) FROM new_table')
print(result.result_rows)
# Output: [(2000, -50.9035)]
