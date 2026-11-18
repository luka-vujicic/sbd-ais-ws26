# Exercise 6 — Card Sentinel

## Part 1 — The decision

I choose **log-based CDC with Debezium** (option 4), because it is the only option that meets R1 and R2 together. The certified payment path stays unchanged, the database gets no extra queries, and every committed payment reaches Kafka in commit order; payments that were rolled back never appear. Polling misses rows that commit out of id order and adds query load for every agent. Dual write and outbox both require changing `card_processor.py`, and dual write can also lose or invent events. The cost of CDC is the replication slot: PostgreSQL keeps all WAL the connector has not confirmed, so if the connector stops for a weekend, the disk fills up and the payment path stops with it. In production I would alert on the WAL retained by the slot (`pg_replication_slots`), on the connector/task status and on free disk space, and cap the risk with `max_slot_wal_keep_size`. If our team owned the payment path, I would choose the **outbox** pattern, because the events then become an explicit contract instead of the internal table layout.

## Part 2 — Three runs

### Run 1 — Debezium's defaults

**Prediction:** All three velocity instances share the work, so 3 × ~450 payments/s is enough for 1,000/s and R4 passes.

**Scoreboard (velocity):**
```
R3  frauds caught          11/34  (32%)   FAIL  (>= 90 %)
R4  alert latency p50/p95  195.9 s / 345.0 s   FAIL  (p95 <= 5 s after the commit)
```

**Why:** Debezium created the topic with **1 partition** and used the primary key `{"id": ...}` as the key. One partition is read by only one instance of a group, so `vel-1` got `[0]`, `vel-2` and `vel-3` got `[]` and processed 0 transactions (scoreboard: `partitions 1`). `vel-1` alone managed ~350 payments/s of 1,000, so its lag grew to ~360 s and R4 failed. R3 failed for the same reason: most frauds were still waiting in the backlog when the run ended. The anomaly model still caught 31/34 (91 %), because its single instance sees every payment.

### Run 2 — 6 partitions

**Prediction:** Now all three instances work, so R4 passes, and the velocity model should also catch its frauds.

**Scoreboard (velocity):**
```
R3  frauds caught          3/7  (43%)   FAIL  (>= 90 %)
R4  alert latency p50/p95  2.3 s / 2.5 s   PASS  (p95 <= 5 s after the commit)
```

**Why:** Each instance got two partitions (`[0, 1]`, `[2, 3]`, `[4, 5]`), shared the load evenly (~47,000 transactions each) and kept up, so R4 passed. But the key was still the payment `id`, and the partition is `hash(key) mod 6`. The 12 test payments of one stolen card were therefore spread over all three instances. Each instance saw only about 4 of them in its 60 s window, below the threshold of 8, so most frauds were missed and R3 failed.

### Run 3 — cardholder as the key

**Prediction:** The key becomes `{"user_id": ...}`, all payments of a card meet in one instance, and R3 passes.

**Scoreboard (velocity):**
```
R3  frauds caught          8/8  (100%)   PASS  (>= 90 %)
R4  alert latency p50/p95  3.3 s / 5.4 s   FAIL  (p95 <= 5 s after the commit)
```

**Why:** With `message.key.columns = public.transactions:user_id`, all payments of a cardholder land in the same partition and are read by the same instance, in commit order. Each instance sees the whole attack and alerts at the 8th payment, so R3 passes with 0 false alerts. The telemetry shows the split too: each velocity instance now saw ~26,000 distinct cardholders instead of ~37,000 in Run 2, because cardholders are no longer shared between instances. R4 missed the limit only narrowly (p95 5.4 s): my laptop runs PostgreSQL, Kafka, Connect and all four agents at once. The fix is capacity, not design: a 4th velocity instance (`agents.py --velocity 4`), which the 6 partitions already allow.

**Note on the anomaly model:** on my laptop, its single instance also fell behind in every run (R4 p95 29–43 s), so frauds still in its backlog counted as missed. That is a capacity problem of the laptop, not of the key, because one instance reads all partitions.

## Closing answer

Debezium chose two defaults that I had to change: **1 partition** and **the primary key as the message key**. Both are fine for copying a table, and both are wrong for a stateful model that has to scale. With one partition, adding instances would not have helped. Lag would have grown at every peak (Run 1: p95 345 s), and stolen cards would have been blocked minutes too late. After the switch to more partitions, the `id` key broke the velocity model *silently*: no crash and no error, just 43 % instead of 100 % of frauds caught. In production that would only show up weeks later in chargebacks. The team could have noticed earlier from consumer lag, from instances with `assigned partitions []`, and from an alert-latency metric. For the key, they could have watched each model's alert rate, backtested against known frauds, or simply looked at the key of one event in the Kafka UI before going live.

