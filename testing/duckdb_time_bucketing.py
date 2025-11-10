import duckdb
from duckdb import CatalogException


db_con = duckdb.connect("duckdb/transactions.db", read_only=True)

TABLE_NAME = "aggregates"

# create_table_query = f"""
# CREATE TABLE {TABLE_NAME} (
#     merchantId        VARCHAR(255)  NOT NULL,
#     totalAmount       DOUBLE PRECISION NOT NULL,
#     transactionCount  BIGINT        NOT NULL,
#     timestamp         TIMESTAMP     NOT NULL,

#     PRIMARY KEY (merchantId, timestamp)
# );
# """
# try: db_con.sql(create_table_query)
# except CatalogException: print(f"{TABLE_NAME} table already exists.")

query = f"""
SET timezone = 'UTC';
SELECT
    time_bucket(INTERVAL '2 minutes', timestamp) AS window_start,
    SUM(totalAmount) AS total_amount
FROM {TABLE_NAME}
WHERE timestamp >= NOW() - INTERVAL '10 minutes'
GROUP BY window_start
ORDER BY window_start;
"""

# query = f"SET timezone='UTC'; Select NOW();"

results = db_con.sql(query).fetchdf()
print(results)