try:
    from kafka import KafkaConsumer
    import json
    import requests
    import os
    import time
    import sys
    import threading

except Exception as e:
    pass

# ------------------------------
os.environ['KAFKA_TOPIC'] = "SMSTopic"

# -----------------------------

def consumer_1(consumer1):
    for msg in consumer1:
        payload = json.loads(msg.value)
        # payload["meta_data"]={
        #     "topic":msg.topic,
        #     "partition":msg.partition,
        #     "offset":msg.offset,
        #     "timestamp":msg.timestamp,
        #     "timestamp_type":msg.timestamp_type,
        #     "key":msg.key,
        # }
        print("Consumer1 ",payload, end="\n")
        time.sleep(1)
        consumer1.commit()


def main():
    print("Listening Consumer 1 *****************")

    consumer1 = KafkaConsumer(
        os.getenv("KAFKA_TOPIC"),
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',#We can change to latest if we want to consume only new messages.
        enable_auto_commit=True,
        group_id='group1'
    )
    consumer_1(consumer1)



main()