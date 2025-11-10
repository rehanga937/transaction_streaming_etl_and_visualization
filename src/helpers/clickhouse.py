from datetime import datetime, timezone

from clickhouse_connect.driver.client import Client


def insert_2_clickhouse(client: Client, item: dict, table_name: str, timestamp: int):
    """Insert a row into a clickhouse table.

    Args:
        client (Client): clickhouse client
        item (dict): The row (key to value map)
        table_name (str): Name of the table
        timestamp (int): Milliseconds since UNIX epoch UTC. 
    """
    dt = datetime.fromtimestamp(timestamp/1000, timezone.utc)

    headers = []
    values = []
    for key, value in item.items():
        if key == 'merchantId': value_ = int(value.split("_")[1])
        else: value_ = value
        headers.append(key)
        values.append(value_)
    
    headers.append('timestamp'); values.append(dt)
    
    client.insert(table_name, [values], column_names=headers)