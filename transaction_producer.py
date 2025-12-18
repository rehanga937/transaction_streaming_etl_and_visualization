import time
import json
import random
import uuid

from confluent_kafka import Producer, Message

from src import config
from src.helpers import generate_amount
from src.helpers.create_topic import create_topic


def produce_event(producer: Producer, event: dict):
    producer.produce(
        topic=config.TRANSACTION_TOPIC,
        value=json.dumps(event).encode("utf-8"),
        # callback=delivery_report # not using a callback to speed things up
    )

def generate_transaction(bias_for_visualization = False) -> dict:
    """Return a randomly generated transaction.

    Args:
        bias_for_visualization (bool): Whether to incorporate a bias into merchant_1 for the purpose of visualization. Defaults to False

    Returns:
        dict: A transaction.
    """
    merchant_id = random.choice(['merchant_1','merchant_2','merchant_3'])

    if bias_for_visualization and merchant_id == 'merchant_1':
        amount = generate_amount.log_normal_amount(mean=20)
    else: amount = generate_amount.log_normal_amount()

    return {
        "transactionId": str(uuid.uuid4()),
        "userId" : f"user_{random.randint(1, 100)}",
        "amount": round(amount, 2),
        "transactionTime": int(time.time()),
        "merchantId": merchant_id,
        "transactionType": random.choice(['purchase','refund']),
        "location": f"location_{random.randint(1, 50)}",
        "paymentMethod": random.choice(['credit_card', 'paypal', 'bank_transfer']),
        "isInternational": random.choice([True, False]),
        "currency": random.choice(['USD', 'EUR', 'LKR'])
    }


def produce_infinite():
    """Produce genenerated transactions to kafka till keyboard interrupt is received.
    """

    producer_config = {
        "bootstrap.servers": config.KAKFA_BROKERS_FOR_LOCAL_MACHINE,
        # "queue.buffering.max.messages": 100000,
        # "queue.buffering.max.kbytes": 512000,
        # "batch.num.messages": 1000,
        "linger.ms": 10,
        "acks": 1,
        "compression.type": "gzip"
    }

    producer = Producer(producer_config)

    create_topic("financial_transactions", 5, 3)

    # produce the events
    tic = time.time(); i = 0; first_tic = time.time(); outlier_count = 0; outlier_ids = []
    while True: # if a BufferError is triggered, producer is flushed and the transaction is retried. (thanks to the while loop and manual incrementing of `i`)
        try:
            try: 
                transaction = generate_transaction(bias_for_visualization=True)
                if transaction['amount'] > 15000: 
                    outlier_count += 1
                    outlier_ids.append(transaction['transactionId'])
            
                produce_event(producer, transaction)
                i += 1

                if i != 0 and i % 200000 == 0:
                    toc = time.time()
                    latest_rate = round(200000 / (toc - tic), 0)
                    overall_rate = round(i / (toc - first_tic), 0)
                    print(f"Produced {i:,} events. Overall rate: {overall_rate:,} tx/s. Latest rate: {latest_rate:,} tx/s. Outliers: {outlier_count}, {outlier_ids}")
                    tic = time.time()
                    outlier_count = 0; outlier_ids = []
            except BufferError: 
                producer.flush()
        except KeyboardInterrupt:
            print("\033[91mStopping continous producer...\033[00m")
            producer.flush()
            break


    
if __name__ == "__main__":
    produce_infinite()
