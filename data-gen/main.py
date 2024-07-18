import time
from faker import Faker
import random
from datetime import datetime
from confluent_kafka import SerializingProducer
import json 

fake = Faker()

def generate_data():
    '''
    '''
    customer = fake.simple_profile()

    price = round(random.uniform(600, 2000), 2)
    qty = random.randint(1, 10)
    return {
        "customerId": customer['username'],
        "productId": f"pr-{random.randint(1, 100)}",
        "productName": random.choice(['iPhone 15', 'iPhone 15 Pro', 'Samsung Galaxy S24', 'Samsung Galaxy S24 Ultra', 'Samsung TV 4k', 'LG TV 2k', 'AirPods Pro 2nd Gen', 'GE Washing Machine']),
        "productPrice": price,
        "productQuantity": qty,
        "totalAmount": round(price * qty, 2),
        "receiptId": fake.uuid4(),
        "receiptDate": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "paymentMethod": random.choice(['Apple Pay', 'Google Pay', 'Credit Card', 'Debit Card', 'Zelle', 'PayPal'])
    }

def delivery_func(err, msg):
    '''
    '''
    if err is not None:
        print(f'Message delivery failed! \n Error: {err}')
    else:
        print(f'Message has been delivered to {msg.topic} [{msg.partition()}]!')

def main():
    # Kafka Constants
    TOPIC_NAME = "customer_transactions"
    producer = SerializingProducer({
        'bootstrap.servers': 'localhost:9092'
    }) 

    for i in range(10):
        try:
            transaction = generate_data()
            print(transaction)
            producer.produce(
                TOPIC_NAME,
                key = transaction["receiptId"],
                value = json.dumps(transaction),
                on_delivery = delivery_func
            )
            producer.poll(0)
            time.sleep(3)
        except BufferError as be:
            time.sleep(5)
            print(f'Internal producer message queue full! Either update this property "queue.buffering.max.messages" or wait for some time!')
        except Exception as e:
            print(f'Exception thrown: {e}')

if __name__ == "__main__":
    main()