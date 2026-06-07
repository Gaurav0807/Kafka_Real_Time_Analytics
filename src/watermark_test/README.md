# Spark Watermark Testing

Test watermark technique in Spark Structured Streaming using click and impression data.

## What is Watermark?

**Watermark** = max(event_time) - threshold

It tells Spark: "Don't expect any more data older than this timestamp"

```
Example:
- Watermark threshold: 2 minutes
- Max event time seen: 10:10:00
- Current watermark: 10:08:00

- Event arrives with event_time = 10:09:00 -> ACCEPTED (after watermark)
- Event arrives with event_time = 10:07:00 -> DROPPED (before watermark)
```

## Why Watermark?

1. **Handle late data**: Real-world data arrives out of order
2. **Clean up state**: Without watermark, Spark keeps ALL aggregation state forever
3. **Enable stream-stream joins**: Required for joining two streams

## Files

| File | Purpose |
|------|---------|
| `impression_producer.py` | Sends impressions to `impressions` topic (20% late events) |
| `click_producer.py` | Sends clicks to `clicks` topic (25% late events) |
| `spark_watermark_simple.py` | Simple watermark demo (single stream) |
| `spark_watermark_join.py` | Stream-stream join with watermarks (CTR calculation) |
| `create_topics.sh` | Create Kafka topics |

## Setup

### 1. Start Docker containers

```bash
cd /Users/grawat/Downloads/Kafka_Real_Time_Analytics-main
docker-compose up -d
```

### 2. Create Kafka topics

```bash
chmod +x src/watermark_test/create_topics.sh
./src/watermark_test/create_topics.sh
```

### 3. Install Python dependencies

```bash
pip install kafka-python pyspark
```

## Running Tests

### Test 1: Simple Watermark (Single Stream)

**Terminal 1 - Start producer:**
```bash
python src/watermark_test/impression_producer.py
```

**Terminal 2 - Start Spark job:**
```bash
# Run inside spark-master container
docker exec -it spark-master bash

# Inside container, run:
spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
    /app_code/watermark_test/spark_watermark_simple.py
```

Or from host (if Spark installed locally):
```bash
spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
    src/watermark_test/spark_watermark_simple.py
```

### Test 2: Stream-Stream Join (Impressions + Clicks)

**Terminal 1 - Impressions:**
```bash
python src/watermark_test/impression_producer.py
```

**Terminal 2 - Clicks:**
```bash
python src/watermark_test/click_producer.py
```

**Terminal 3 - Spark job:**
```bash
docker exec -it spark-master bash

spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
    /app_code/watermark_test/spark_watermark_join.py
```

## What to Observe

### In Producers
- Watch for `[LATE]` markers - these events have old event_time

### In Spark Output
- **Window aggregations**: Data grouped by time windows
- **Late events**: May be included or dropped based on watermark
- **CTR calculation**: clicks / impressions * 100

## Watermark Behavior Summary

| Scenario | Watermark | Behavior |
|----------|-----------|----------|
| `withWatermark("event_ts", "2 minutes")` | 2 min | Events >2 min late are dropped |
| `withWatermark("event_ts", "10 seconds")` | 10 sec | Very strict, most late events dropped |
| `withWatermark("event_ts", "1 hour")` | 1 hour | Very lenient, keeps more state in memory |
| No watermark | None | Keeps ALL state forever (memory grows infinitely) |

## Architecture

```
┌─────────────────┐     ┌─────────────────┐
│ impression_     │     │ click_          │
│ producer.py     │     │ producer.py     │
└────────┬────────┘     └────────┬────────┘
         │                       │
         ▼                       ▼
┌─────────────────────────────────────────┐
│           Kafka Broker                  │
│  ┌───────────┐      ┌───────────┐      │
│  │impressions│      │  clicks   │      │
│  │   topic   │      │   topic   │      │
│  └───────────┘      └───────────┘      │
└─────────────────────────────────────────┘
         │                       │
         └───────────┬───────────┘
                     ▼
┌─────────────────────────────────────────┐
│      Spark Structured Streaming         │
│  ┌─────────────────────────────────┐   │
│  │  Watermark (handle late data)    │   │
│  ├─────────────────────────────────┤   │
│  │  Stream-Stream JOIN              │   │
│  │  (impressions ⟕ clicks)          │   │
│  ├─────────────────────────────────┤   │
│  │  Windowed Aggregation            │   │
│  │  (CTR per ad per minute)         │   │
│  └─────────────────────────────────┘   │
└─────────────────────────────────────────┘
                     │
                     ▼
              Console Output
```
