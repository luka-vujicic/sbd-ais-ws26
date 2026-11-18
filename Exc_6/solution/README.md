# Exercise 6 — Card Sentinel

## Part 1 — The decision

I choose **log-based CDC with Debezium** (option 4), because it is the only option that meets R1 and R2 together. The certified payment path stays unchanged, the database gets no extra queries, and every committed payment reaches Kafka in commit order. Payments that were rolled back never appear. Polling misses rows that commit out of id order and adds query load for every agent. Dual write and outbox both require changing `card_processor.py`, and dual write can also lose or invent events. The cost of CDC is the replication slot: PostgreSQL keeps all WAL the connector has not confirmed. If the connector stops for a weekend, the disk fills up and the payment path stops with it. In production I would alert on the WAL retained by the slot (`pg_replication_slots`), on the connector/task status, and on free disk space. I would also cap the risk with `max_slot_wal_keep_size`. If our team owned the payment path, I would choose the **outbox** pattern, because the events then become an explicit contract instead of the internal table layout.

## Part 2 — Three runs

### Run 1 — Debezium's defaults

**Prediction:** <!-- your own guess, e.g. "all three velocity instances share the work and R4 passes" -->

**Scoreboard (velocity):**
```
R3: <paste line>
R4: <paste line>
```

**Why:** Debezium created the topic with **1 partition**, and the key of each event was the primary key `{"id": ...}`. Within a consumer group, one partition is read by exactly one instance. So `vel-1` got `[0]`, while `vel-2` and `vel-3` got `[]` and stayed idle (scoreboard: `members 3`, `partitions 1`). One instance handles ~400–500 payments/s of 1,000, so the lag grew every second and R4 failed. The frauds of the last minute were still waiting in the backlog. The anomaly model passed: at 0.3 ms per payment, one instance keeps up.

### Run 2 — 6 partitions

**Prediction:** <!-- your own guess -->

**Scoreboard (velocity):**
```
R3: <paste line>
R4: <paste line>
```

**Why:** Each velocity instance got two partitions and kept up, so R4 passed. But the key was still the payment `id`, and the partition is `hash(key) mod 6`. So the 12 test payments of one stolen card were spread over all three instances. Each instance saw only about 4 of them in its 60 s window, below the threshold of 8, so almost no fraud was caught and R3 failed. The anomaly model still passed, only because its single instance reads all six partitions. With two instances it would fail the same way.

### Run 3 — cardholder as the key

**Prediction:** <!-- your own guess, e.g. "the key becomes {\"user_id\": ...} and R3 passes" -->

**Scoreboard (velocity):**
```
R3: <paste line>
R4: <paste line>
```

**Why:** With `message.key.columns = public.transactions:user_id`, the key is now `{"user_id": ...}`. All payments of a cardholder land in the same partition and are read by the same instance, in commit order. So each instance sees the whole attack and alerts at the 8th payment: R3 passes. The six partitions still share the load, so R4 passes, and every model gets every payment (R2). <!-- if you got a false alert: --> The occasional false alert is an honest cardholder who paid 8 times within a minute. A threshold rule cannot tell them apart from a thief.

## Closing answer

Debezium chose two defaults that I had to change: **1 partition** and **the primary key as the message key**. Both are fine for copying a table, and both are wrong for a stateful model that has to scale. With one partition, adding instances would not have helped. Lag would have grown at every peak, and stolen cards would have been blocked minutes too late. After the switch to more partitions, the `id` key would have broken the velocity model *silently*: no crash, no error, just far fewer frauds caught, which would only show up weeks later in chargebacks. The team could have noticed earlier from consumer lag, from instances with `assigned partitions []`, and from an alert-latency metric. For the key, they could have watched each model's alert rate, backtested against known frauds, or simply looked at the key of one event in the Kafka UI before going live.