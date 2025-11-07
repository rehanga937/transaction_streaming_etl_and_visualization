#!/bin/bash

# wait till at least this kafka broker is ready
until (echo > /dev/tcp/kafka-broker-1/19092) >/dev/null 2>&1; do
      sleep 5;
done;

# submit the spark job
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 \
  --conf spark.jars.ivy=/tmp/.ivy2 /opt/spark/work-dir/spark_jobs.py

exit 0;