"""
Impression Event Producer

Sends impression events to Kafka topic 'impressions'.
Impressions = Ad shown to user (happens FIRST, before click)

Schema:
- impression_id: unique ID
- ad_id: which ad was shown
- user_id: who saw it
- event_time: when impression happened
"""

import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC = "impressions"

# Sample data
AD_IDS = ["ad_001", "ad_002", "ad_003"]
USER_IDS = ["user_001", "user_002", "user_003", "user_004", "user_005"]
PAGES = ["/home", "/products", "/checkout"]

# Store recent impressions for click producer to reference
RECENT_IMPRESSIONS = []


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


def generate_impression():
    """Generate an impression event."""
    now = datetime.now()
    impression_id = f"imp_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
    ad_id = random.choice(AD_IDS)
    user_id = random.choice(USER_IDS)

    impression = {
        "impression_id": impression_id,
        "ad_id": ad_id,
        "user_id": user_id,
        "event_time": now.strftime("%Y-%m-%d %H:%M:%S"),
        "page_url": random.choice(PAGES),
        "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
    }

    # Save for click producer reference (write to shared file)
    save_impression_for_clicks(impression)

    return impression


def save_impression_for_clicks(impression):
    """Save impression to file so click producer can reference it."""
    try:
        with open("/tmp/recent_impressions.jsonl", "a") as f:
            f.write(json.dumps(impression) + "\n")
    except Exception:
        pass


def main():
    producer = create_producer()
    print(f"Starting Impression Producer -> Topic: {TOPIC}")
    print("Impressions will be saved for click producer to reference")
    print("Press Ctrl+C to stop\n")

    count = 0
    while True:
        try:
            impression = generate_impression()

            producer.send(TOPIC, key=impression["ad_id"], value=impression)

            print(f"[{count}] IMPRESSION: {impression['impression_id'][:20]}... | "
                  f"ad={impression['ad_id']} | user={impression['user_id']} | "
                  f"time={impression['event_time']}")

            count += 1
            time.sleep(random.uniform(1.0, 3.0))  # 1-3 seconds between impressions

        except KeyboardInterrupt:
            print(f"\nStopped. Total impressions sent: {count}")
            break

    producer.close()


if __name__ == "__main__":
    main()
