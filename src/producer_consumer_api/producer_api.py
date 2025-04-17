from kafka import KafkaProducer
import logging
import time
import json
import random
import uuid
from faker import Faker

logging.basicConfig(level=logging.INFO)

fake = Faker()

def create_kafka_producer(bootstrap_servers):
    try:
        producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize data as JSON
        )
        logging.info("Kafka Producer created successfully")
        return producer
    except Exception as e:
        logging.error(f"Failed to create Kafka producer: {e}")
        return None
    
def send_data_to_kafka(kafka_producer, dict_input, kafka_topic ):

    if not kafka_producer:
        logging.info("No Kafka Producer is their")
        return
    # try:
    #     for user in dict_input:
    #         time.sleep(10)
    #         kafka_producer.send(kafka_topic, user)
    #         logging.info(f"Sent user data to kafka topic : {kafka_topic}")
    #         print("Data Send to kafka Topic")
    #         print(user)
    #     kafka_producer.flush()
    # except Exception as e:
    #     logging.error(f"Failed to send data to Kafka: {e}")
    
    # print(dict_input)
    try:
        #time.sleep(10)
        kafka_producer.send(kafka_topic, dict_input)
        logging.info(f"Sent user data to kafka topic : {kafka_topic}")
        print("Data Send to kafka Topic")
        print(dict_input)
        kafka_producer.flush()
    except Exception as e:
         logging.error(f"Failed to send data to Kafka: {e}")



def generate_finance_data():

    return dict(
        transactionId = str(uuid.uuid4()),
        name = fake.name(),
        userId = f"user_{random.randint(1,100)}",
        amount = round(random.randint(1000,200000)*2),
        transactiontime = time.ctime(),
        transactionType = random.choice(['Purchase','Refund']),
        city = fake.city()

    )





if __name__ == "__main__":
    
    # Define your Kafka configuration
    bootstrap_servers = '127.0.0.1:9092'
    kafka_topic = 'users_input'

    while True:
        # id = int(input(" Enter the id :- "))
        # name = input("Enter the name :- ")

        # dict_input = {
        #     "id":id,
        #     "name":name,
        #     "record_process_time": time.ctime()
        # }
        dict_input = generate_finance_data()
        time.sleep(5)

        if dict_input:
            kafka_producer = create_kafka_producer(bootstrap_servers)

            send_data_to_kafka(kafka_producer, dict_input, kafka_topic)


