# Why Do We Need Watermark If We Already Have a Join Condition?

This is one of the most common Spark Structured Streaming interview questions.

Many developers ask:

> If I already have a join condition like:

```sql
c.event_time <= i.event_time + interval 2 minutes
```

why do I still need watermark?

---

# Short Answer

The join condition tells Spark:

> Which records can match.

The watermark tells Spark:

> When old records can be safely removed from memory.

They solve different problems.

---

# Example

## Impression Stream

| impression_id | event_time |
|--------------|------------|
| 1001 | 10:00 |
| 1002 | 10:01 |
| 1003 | 10:04 |

## Click Stream

| impression_id | event_time |
|--------------|------------|
| 1001 | 10:02 |
| 1002 | 10:02 |

---

# Join Condition

```sql
i.impression_id = c.impression_id
AND c.event_time >= i.event_time
AND c.event_time <= i.event_time + interval 2 minutes
```

Business meaning:

> A click must happen within 2 minutes of the impression.

---

# Record 1001

Impression:

```text
1001 10:00
```

Click:

```text
1001 10:02
```

Check:

```text
10:02 <= 10:02
```

Result:

```text
MATCH
```

---

# Record 1002

Impression:

```text
1002 10:01
```

Click:

```text
1002 10:02
```

Check:

```text
10:02 <= 10:03
```

Result:

```text
MATCH
```

---

# The Real Problem

Suppose only this impression arrives:

```text
1001 10:00
```

Spark stores it in memory:

```text
State Store

1001 -> 10:00
```

No click has arrived yet.

---

# Question

When should Spark delete this record?

```text
1001 -> 10:00
```

Options:

```text
10:02 ?
10:05 ?
10:30 ?
Tomorrow ?
```

How does Spark know?

---

# Why Spark Cannot Decide

Imagine a click happened at:

```text
event_time = 10:01
```

But because of network delay it arrives at:

```text
arrival_time = 10:05
```

The click is still valid because:

```text
10:01 <= 10:02
```

Therefore Spark cannot immediately remove:

```text
1001 -> 10:00
```

from memory.

---

# Without Watermark

Spark does not know:

```text
How late can data arrive?
```

Maybe:

```text
1 minute late
```

Maybe:

```text
10 minutes late
```

Maybe:

```text
1 hour late
```

Maybe:

```text
Tomorrow
```

Spark has no idea.

Therefore it keeps state forever.

---

# Memory Growth

Without watermark:

```text
1001 -> 10:00
1002 -> 10:01
1003 -> 10:02
1004 -> 10:03
...
```

State keeps growing.

Eventually:

```text
Huge memory usage
Slow processing
Possible OutOfMemory
```

---

# Watermark

```python
.withWatermark("event_time", "5 minutes")
```

This tells Spark:

> Events can be at most 5 minutes late.

Now Spark has a rule.

---

# What Spark Knows Now

Impression:

```text
1001 10:00
```

Join window:

```text
10:00 → 10:02
```

Allowed lateness:

```text
5 minutes
```

Therefore Spark can safely remove state after:

```text
10:02 + 5 minutes
=
10:07
```

because the user promised:

```text
No valid click will arrive after that.
```

---

# What Watermark Does

Watermark is NOT:

```text
Join Logic
```

Watermark IS:

```text
State Cleanup Logic
```

---

# Difference Between Join Condition and Watermark

## Join Condition

```sql
c.event_time <= i.event_time + interval 2 minutes
```

Purpose:

```text
Which records can join?
```

---

## Watermark

```python
.withWatermark("event_time", "5 minutes")
```

Purpose:

```text
How long should Spark keep old records?
```

---

# Simple Analogy

Think of Spark as a cache.

It stores:

```text
1001 -> 10:00
```

The join condition tells Spark:

```text
When a click matches.
```

The watermark tells Spark:

```text
When it is safe to delete the cached record.
```

---

# Interview Answer

Why do we need watermark if we already have a join condition?

Answer:

The join condition defines which events are eligible to match. Watermark defines how long Spark should retain state and wait for late-arriving events. Without watermark, Spark cannot safely clean old state because it does not know how late future events may arrive. As a result, state would grow indefinitely.