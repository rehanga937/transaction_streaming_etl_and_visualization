#!/bin/bash

# create the topics
//bin/kafka-topics --create --topic financial_transactions --bootstrap-server kafka-broker-1:19092,kafka-broker-2:19092,kafka-broker-3:19092 --partitions 5 --replication-factor 3;
//bin/kafka-topics --create --topic transaction_aggregates --bootstrap-server kafka-broker-1:19092,kafka-broker-2:19092,kafka-broker-3:19092 --partitions 1 --replication-factor 3;
//bin/kafka-topics --create --topic transaction_anomalies --bootstrap-server kafka-broker-1:19092,kafka-broker-2:19092,kafka-broker-3:19092 --partitions 1 --replication-factor 3;

# delete messages from financial_transactions topics once the topic storage use exceeds given amount
//bin/kafka-configs --bootstrap-server kafka-broker-1:19092,kafka-broker-2:19092,kafka-broker-3:19092 \
  --alter --entity-type topics --entity-name financial_transactions --add-config retention.bytes=1073741824


