#!/bin/bash

# create the topics
//bin/kafka-topics --create --topic financial_transactions --bootstrap-server kafka-broker-1:19092,kafka-broker-2:19092,kafka-broker-3:19092 --partitions 5 --replication-factor 3;
//bin/kafka-topics --create --topic transaction_aggregates --bootstrap-server kafka-broker-1:19092,kafka-broker-2:19092,kafka-broker-3:19092 --partitions 1 --replication-factor 3;
//bin/kafka-topics --create --topic transaction_anomalies --bootstrap-server kafka-broker-1:19092,kafka-broker-2:19092,kafka-broker-3:19092 --partitions 1 --replication-factor 3;

# delete messages from financial_transactions topics once the topic storage use exceeds given amount
# https://kafka.apache.org/documentation/#topicconfigs
# retain set to 100 MB. This means with 5 partitions and replication factor of 3, total retention = 1.5 GB
# with the delete delay, total size might reach around 2.6 GB before the deletion resets the storage use back to 1.5 GB.
//bin/kafka-configs --bootstrap-server kafka-broker-1:19092,kafka-broker-2:19092,kafka-broker-3:19092 \
  --alter --entity-type topics --entity-name financial_transactions --add-config segment.bytes=52428800 # 50 MB

//bin/kafka-configs --bootstrap-server kafka-broker-1:19092,kafka-broker-2:19092,kafka-broker-3:19092 \
  --alter --entity-type topics --entity-name financial_transactions --add-config retention.bytes=104857600 # 100 MB

//bin/kafka-configs --bootstrap-server kafka-broker-1:19092,kafka-broker-2:19092,kafka-broker-3:19092 \
  --alter --entity-type topics --entity-name financial_transactions --add-config log.cleaner.enable=true

//bin/kafka-configs --bootstrap-server kafka-broker-1:19092,kafka-broker-2:19092,kafka-broker-3:19092 \
  --alter --entity-type topics --entity-name financial_transactions --add-config cleanup.policy=delete