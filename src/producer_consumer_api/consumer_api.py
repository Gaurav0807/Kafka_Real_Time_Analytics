from kafka import KafkaConsumer
import json
import logging

logging.basicConfig(level=logging.INFO)

KAFKA_TOPIC = 'users_input'
BOOTSTRAP_SERVERS = ['127.0.0.1:9092']


consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id='user-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

logging.info("Kafka Consumer started. Waiting for messages...")

try:
    for message in consumer:
        logging.info("Received message:")
        print(json.dumps(message.value, indent=2))
except KeyboardInterrupt:
    logging.info("Shutting down consumer.")