# Intro
This directory contains a project to demo **streaming ETL and visualization of synthesized transactions**.

1. A python script is used to generate synthetic transactions and send them all to a Kafka topic.
2. A spark streaming job is used to perform transformations on the transaction events. 
    - Aggregate performance per merchant (total amount and transaction count) within a time slot.
    - Anomalous transactions
    
    The transformed data is written to their own kafka topics.
3. A python script consumes messages from the transformed data's topics and writes them to a clickhouse table.
4. Apache Superset can be used to connect to the clickhouse database to visualize the data.

![architecture_flowchart.jpg](readme_images/architecture_flowchart.jpg)

# Setup
`Python 3.11.14` was used for this project. Other Python versions were not tested. 
1. Build the docker images for the docker container group
    ```bash
    docker compose build
    ```
2. Install the required python packages for the pythons script.
    ```bash
    pip install -r requirements.txt
    ```
    Or use the file with package versions frozen:
    ```bash
    pip install -r requirements_frozen.txt
    ```
3. Have the environmental variables ready (e.g. include these in a `.env` file):
    - CLICKHOUSE_DB - name of the DB in clickhouse to be created and used
    - CLICKHOUSE_PORT - should be 8123
    - CLICKHOUSE_USER - username to be created for clickhouse
    - CLICKHOUSE_PASSWORD - - password to be created for clickhouse
1. Have Superset ready with clickhouse drivers installed.
    This project was done in a Windows environment. Since Superset doesn't support Windows, it was run from a docker image.
    
    **What was done for Windows:**

    The instructions from https://superset.apache.org/docs/6.0.0/quickstart/ and https://superset.apache.org/docs/6.0.0/configuration/databases/ were used to run Superset in development mode via docker.
    - Superset github repo was cloned
        ```bash
        git clone https://github.com/apache/superset
        ```
    - Version 5.0.0 was used.
        ```bash
        git checkout tags/5.0.0
        ```
    - 'clickhouse-connect>=0.6.8' was written to `docker/requirements-local.txt` (file may have to be created). This ensures that the clickhouse drivers will be available to Superset.
    - Now Superset can be launched:
        ```bash
        docker compose -f docker-compose-image-tag.yml up
        ```
        The default credentials can be used (admin for both username and password)
        
# Usage
## Get the stuff running
1. Run the docker container group with the Kafka cluster, Redpanda console for Kafka, Spark cluster and Clickhouse DB.
    ```bash
    docker compose up
    ```
    The spark streaming job (`spark-jobs/spark_jobs.py`) is automatically submitted to the spark cluster.

    This can take a while, wait for the container group to be up and running before following the remaining steps.
2. Launch the 3 python scripts
    ```bash
    python3 transaction_producer.py
    ```
    ```bash
    python3 merchant_performance_consumer.py
    ```
    ```bash
    python3 anomaly_consumer.py
    ```
3. Connect Superset to the clickhouse database
    - If using Superset on windows using the above method (docker container in development mode), db host address must be `host.docker.internal:8123`.
    
    
## For a fresh start
For a fresh start:
- delete the existing related docker volumes
- delete the `mnt/` folder from the project directory

> [!TIP]
> Clean shutdowns by exiting the producer and consumer python programs by pressing ctrl+c, and winding down the container group using 
> ```bash
> docker compose down
> ```
> will negate the need to create a fresh start.

    