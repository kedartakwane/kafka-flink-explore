import time
from faker import Faker
import random
from datetime import datetime
from confluent_kafka import SerializingProducer
import json 
import threading

# Kafka Constants
TRANSACTIONS_TOPIC_NAME = "transactions_topic"
CUSTOMER_TOPIC_NAME = "customer_topic"
KAFKA_BOOTSTRAP_SERVER = "localhost:9092"

# Iteration Constants
NUM_OF_RECORDS = 100
SLEEP_AFTER = 20
SLEEP_FOR_SEC = 20

# Faker
fake = Faker()

# Customer related fields
customer_list = []

def delivery_func(err, msg):
    '''
    '''
    if err is not None:
        print(f'Message delivery failed! \n Error: {err}')
    else:
        print(f'Message has been delivered to {msg.topic} [{msg.partition()}]!')

def get_customer_record():
    '''
    '''
    customer = fake.simple_profile()

    return {
        "customerId": customer["username"],
        "name": customer["name"],
        "email": customer["mail"],
        "birthdate": customer["birthdate"].strftime('%Y-%m-%d'),
    }

def generate_customer_data(topic):
    '''
    '''
    # Producer obj
    producer = SerializingProducer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER
    }) 

    print(f'>>> Producing Customers to {topic}!')
    # Producing customers to Kafka topic
    for _ in range(NUM_OF_RECORDS):
        if _ != 0 and NUM_OF_RECORDS > SLEEP_AFTER and _ % SLEEP_FOR_SEC == 0:
            print(f'>>> Waiting for {SLEEP_FOR_SEC} seconds before producing more records.')
            time.sleep(SLEEP_FOR_SEC)
        customer = get_customer_record()
        customer_list.append(customer)
        try:
            print(f'Customer || {customer}')
            producer.produce(
                topic,
                key = customer["customerId"],
                value = json.dumps(customer),
                on_delivery = delivery_func
            )
            producer.poll(0)
            time.sleep(1)
        except BufferError as be:
            time.sleep(5)
            print(f'generate_customer_data() || Internal producer message queue full! Either update this property "queue.buffering.max.messages" or wait for some time!')
        except Exception as e:
            print(f'generate_customer_data() || Exception thrown: {e}')

def get_transaction_record():
    '''
    '''
    price = round(random.uniform(600, 2000), 2)
    qty = random.randint(1, 10)
    return {
        "customerId": random.choice(customer_list)['customerId'],
        "productId": f"pr-{random.randint(1, 100)}",
        "productName": random.choice(['iPhone 15', 'iPhone 15 Pro', 'Samsung Galaxy S24', 'Samsung Galaxy S24 Ultra', 'Samsung TV 4k', 'LG TV 2k', 'AirPods Pro 2nd Gen', 'GE Washing Machine']),
        "productPrice": price,
        "productQuantity": qty,
        "totalAmount": round(price * qty, 2),
        "receiptId": fake.uuid4(),
        "receiptDate": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "paymentMethod": random.choice(['Apple Pay', 'Google Pay', 'Credit Card', 'Debit Card', 'Zelle', 'PayPal'])
    }

def generate_transaction_data(topic):
    '''
    '''
    # Producer obj
    producer = SerializingProducer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER
    }) 
    print(f'>>> Producing Transactions to {topic}!')
    # Producing transaction data to Kafka topic
    for _ in range(NUM_OF_RECORDS):
        if _ != 0 and NUM_OF_RECORDS > SLEEP_AFTER and _ % SLEEP_FOR_SEC == 0:
            print(f'>>> Waiting for {SLEEP_FOR_SEC} seconds before producing more records.')
            time.sleep(SLEEP_FOR_SEC)
        try:
            transaction = get_transaction_record()
            print(f'Transaction || {transaction}')
            producer.produce(
                topic,
                key = transaction["receiptId"],
                value = json.dumps(transaction),
                on_delivery = delivery_func
            )
            producer.poll(0)
            time.sleep(1)
        except BufferError as be:
            time.sleep(5)
            print(f'generate_transaction_data() || Internal producer message queue full! Either update this property "queue.buffering.max.messages" or wait for some time!')
        except Exception as e:
            print(f'generate_transaction_data() || Exception thrown: {e}')

def main():
    # Generate customer data and transaction data
    t1 = threading.Thread(target=generate_customer_data, args=[CUSTOMER_TOPIC_NAME])
    t2 = threading.Thread(target=generate_transaction_data, args=[TRANSACTIONS_TOPIC_NAME])

    # Start threads
    t1.start()
    t2.start()

    # Await their completion
    t1.join()
    t2.join()

    print(f'Data generated! Check Kafka for messages produced.')

if __name__ == "__main__":
    main()