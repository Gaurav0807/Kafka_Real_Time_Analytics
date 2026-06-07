Spark Structured Streaming: Stream-Stream Join with Watermark
Problem Statement

We have two Kafka streams:

## Impression Stream


| impression_id | event_time |
|------|---------|
| `1001` | 10:00 |
| `1002` | 10:01 |
| `1003` | 10:04 |


## Click Stream


| impression_id | event_time |
|------|---------|
| `1001` | 10:02 |
| `1002` | 10:02 |



Business requirement:

Join impressions and clicks using impression_id
A click is valid only if it occurs within 2 minutes of the impression
Handle late arriving events
Prevent Spark from storing state forever
Why Stream-Stream Join Needs State

When an impression arrives:

1001 10:00

Spark cannot immediately determine whether a matching click exists.

Therefore Spark stores the impression in memory (State Store).

State Store

1001 -> 10:00

Later a click may arrive:

1001 10:02

Spark performs the join.

Without cleanup, state would continue growing forever.

Solution
Step 1: Add Watermark
```
impression_df = impressions.withWatermark(
    "event_time",
    "5 minutes"
)
```

```
click_df = clicks.withWatermark(
    "event_time",
    "5 minutes"
)
```

Watermark tells Spark:

Data may arrive up to 5 minutes late.

Step 2: Add Time Range Join
i.impression_id = c.impression_id

AND

c.event_time >= i.event_time

AND

c.event_time <= i.event_time + interval 2 minutes

Meaning:

A click must happen within 2 minutes after the impression.

Full Join Code
joined_df = (
    impression_df.alias("i")
    .join(
        click_df.alias("c"),
        expr("""
            i.impression_id = c.impression_id
            AND c.event_time >= i.event_time
            AND c.event_time <= i.event_time + interval 2 minutes
        """),
        "inner"
    )
)
Example Walkthrough
Batch 1
Impressions
1001 10:00
1002 10:01
1003 10:04
Clicks
1001 10:02
1002 10:02
Impression Watermark

Maximum event time:

10:04

Watermark:

10:04 - 5 minutes
=
09:59
Click Watermark

Maximum event time:

10:02

Watermark:

10:02 - 5 minutes
=
09:57
Join Evaluation
Record 1001

Impression:

10:00

Click:

10:02

Validation:

10:02 >= 10:00

TRUE

10:02 <= 10:02

TRUE

Result:

MATCH
Record 1002

Impression:

10:01

Click:

10:02

Validation:

10:02 >= 10:01

TRUE

10:02 <= 10:03

TRUE

Result:

MATCH
Record 1003

Impression:

10:04

No click exists yet.

Spark keeps this record in state.

Why Do We Need The Time Condition?

Suppose we only use:

i.impression_id = c.impression_id

Then Spark cannot know whether a click may arrive:

10 minutes later
1 hour later
1 day later

State would grow forever.

The time condition tells Spark:

A click can only occur within 2 minutes.

Why Do We Need Watermark?

Many people ask:

If I already have a 2-minute join condition, why add watermark?

Because they solve different problems.

Time Condition
c.event_time <= i.event_time + interval 2 minutes

Purpose:

Business Logic

Determines:

Which records can join
Watermark
.withWatermark("event_time", "5 minutes")

Purpose:

State Cleanup

Determines:

How long Spark waits for late events
Late Event Example

Current state:

Max Impression Event Time = 10:20

Impression watermark:

10:15

Now Spark receives:

1001 10:02

Check:

10:02 < 10:15

Record is older than watermark.

Result:

DROP

Spark ignores it.

Trigger vs Watermark
Trigger
.trigger(processingTime="10 seconds")

Controls:

When Spark runs a micro-batch

Example:

10:00:00
10:00:10
10:00:20
...
Watermark
.withWatermark("event_time", "5 minutes")

Controls:

How long Spark remembers old events
Interview Summary
Why use a time-range join?

To define valid business relationships between events.

Example:

Click must occur within 2 minutes of impression.
Why use watermark?

To handle late arriving events and clean old state.

What is watermark?
Watermark =
Maximum Event Time Seen So Far
-
Allowed Lateness

Example:

Max Event Time = 10:20

Watermark Delay = 5 minutes

Watermark = 10:15

Records older than 10:15 may be removed.

Key Difference
Feature	Purpose
Time Range Join	Defines which records can match
Watermark	Defines how long Spark keeps state
Trigger	Defines how often Spark processes data
One-Line Interview Answer

Watermark is used to handle late-arriving events and bound state size in Spark Structured Streaming. The join condition defines which events can match, while the watermark defines how long Spark waits before safely removing old state.