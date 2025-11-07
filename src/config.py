KAKFA_BROKERS_FOR_LOCAL_MACHINE = "localhost:29092,localhost:39092,localhost:49092"
KAKFA_BROKERS = "kafka-broker-1:19092,kafka-broker-2:19092,kafka-broker-3:19092"


# if updating topic names, update kafka-init/init.sh
TRANSACTION_TOPIC = "financial_transactions"
AGGREGATES_TOPIC = "transaction_aggregates"
ANOMALIES_TOPIC = "transaction_anomalies"