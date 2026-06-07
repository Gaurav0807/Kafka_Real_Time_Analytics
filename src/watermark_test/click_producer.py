"""
Click Event Producer

Sends click events to Kafka topic 'clicks'.
Clicks happen 3-5 minutes AFTER impression (realistic user behavior)

LATE ARRIVAL: 20% of clicks have additional network delay (arrive late)

Timeline Example:
- 10:00:00 - User sees ad (impression)
- 10:03:30 - User clicks ad (3.5 min later) - event_time = 10:03:30
- 10:04:00 - Click data arrives at Kafka (30 sec network delay)

With LATE arrival:
- 10:00:00 - User sees ad (impression)
- 10:04:00 - User clicks ad (4 min later) - event_time = 10:04:00
- 10:06:30 - Click data arrives at Kafka (2.5 min network delay!) [LATE]
"""

import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC = "clicks"


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


def load_recent_impressions():
    """Load recent impressions from shared file."""
    impressions = []
    try:
        with open("/tmp/recent_impressions.jsonl", "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    impressions.append(json.loads(line))
        # Keep only last 50 impressions
        return impressions[-50:]
    except FileNotFoundError:
        return []
    except Exception as e:
        print(f"Error loading impressions: {e}")
        return []


def generate_click(impression, late_arrival=False):
    """
    Generate a click event based on an impression.

    Args:
        impression: The impression this click is for
        late_arrival: If True, simulate network delay (data arrives late)
    """
    # Parse impression time
    imp_time = datetime.strptime(impression["event_time"], "%Y-%m-%d %H:%M:%S")

    # Click happens 5-15 seconds after impression (fast for demo)
    click_delay_seconds = random.randint(5, 15)
    click_time = imp_time + timedelta(seconds=click_delay_seconds)

    # When does the click DATA arrive at Kafka?
    if late_arrival:
        # Late arrival: additional 10-30 second delay for demo
        arrival_delay = random.randint(10, 30)
        arrival_time = click_time + timedelta(seconds=arrival_delay)
    else:
        # Normal: 1-3 second network delay
        arrival_delay = random.randint(1, 3)
        arrival_time = click_time + timedelta(seconds=arrival_delay)

    click_id = f"clk_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"

    return {
        "click_id": click_id,
        "impression_id": impression["impression_id"],
        "ad_id": impression["ad_id"],
        "user_id": impression["user_id"],
        "event_time": click_time.strftime("%Y-%m-%d %H:%M:%S"),  # When click happened
        "timestamp": arrival_time.strftime("%Y-%m-%d %H:%M:%S"),  # When data arrived
    }, late_arrival, click_delay_seconds


def main():
    producer = create_producer()
    print(f"Starting Click Producer -> Topic: {TOPIC}")
    print("Clicks happen 3-5 minutes after impressions")
    print("20% of clicks have LATE arrival (network delay)")
    print("Press Ctrl+C to stop\n")
    print("Waiting for impressions...")

    count = 0
    processed_impressions = set()

    while True:
        try:
            # Load recent impressions
            impressions = load_recent_impressions()

            if not impressions:
                print("No impressions yet. Waiting...")
                time.sleep(5)
                continue

            # Find unprocessed impressions
            for imp in impressions:
                imp_id = imp["impression_id"]

                if imp_id in processed_impressions:
                    continue

                # 60% chance user clicks the ad (realistic CTR is much lower, but for demo)
                if random.random() > 0.6:
                    processed_impressions.add(imp_id)
                    continue

                # 20% chance of late arrival
                is_late = random.random() < 0.2

                click, late, delay_sec = generate_click(imp, late_arrival=is_late)

                producer.send(TOPIC, key=click["ad_id"], value=click)

                late_marker = " [LATE ARRIVAL]" if late else ""
                print(f"[{count}] CLICK: {click['click_id'][:20]}... | "
                      f"ad={click['ad_id']} | user={click['user_id']} | "
                      f"click_time={click['event_time']} | "
                      f"delay={delay_sec//60}m{delay_sec%60}s{late_marker}")

                processed_impressions.add(imp_id)
                count += 1

            time.sleep(2)

        except KeyboardInterrupt:
            print(f"\nStopped. Total clicks sent: {count}")
            break

    producer.close()


if __name__ == "__main__":
    main()
