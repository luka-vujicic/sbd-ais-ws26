# Exercise 5 - Line Guardian

## Stage A - Today's system

### A1 - 20 machines (200 readings/s)

Run 1794155356 (95 s, first fault on M-006 at t = 40 s):
- readings sent by plant: 18,960
- rows in table (`SELECT count(*) FROM readings WHERE run = ...`): 18,960
- id range covered by detector: 33,040 to 52,000 (18,960 ids)
- readings read by detector: 16,390
- missing reported by model: 2,554 (~13.5%)

With 20 machines at 10 Hz (200 readings/s), one detector is fast enough (1 / 3 ms = 333 readings/s max). Backlog stayed between 0 and 56 rows and delay stayed <= 0.4 s, but `missing` still grew by 130-150 every 5 s.

Are the missing readings in the table?
Yes. All 18,960 rows are in PostgreSQL, and read (16,390) + missing (2,554) adds up to the total written. You can also see the skipped sequence numbers directly in the detector output for M-006 (jumps from seq 483 to 485, 490 to 492, 754 to 758).

Why did the detector never see them?
`id` is a `BIGSERIAL`, so PostgreSQL assigns the id on `INSERT`, but other connections only see the row after `COMMIT`. `PostgresSink` uses 4 gateways that sleep 0-120 ms between insert and commit, meaning transactions commit out of id order.

Following the timeline from the task:
1. Gateway 1 inserts ids 100-114, still uncommitted.
2. Gateway 2 inserts ids 115-129 and commits immediately.
3. The detector polls `WHERE id > 99`. It only sees committed rows 115-129 and updates `last_id = 129`.
4. Gateway 1 commits 100 ms later. Rows 100-114 are now visible in the table.
5. On the next poll, the detector queries `WHERE id > 129`, so rows 100-114 are permanently skipped. The model sees a gap in `seq` and counts them as missing. This breaks R2.


### A2 - Full plant, 60 machines (600 readings/s)

Run 1794155600 (81 s, 48,300 readings sent, fault on M-029 at t = 40 s):

```text
report 1:  read 1,676  | backlog 2,071 rows  | delay 3.6 s  | missing 19
report 2:  read 3,676  | backlog 4,496 rows  | delay 7.6 s  | missing 19
report 4:  read 7,676  | backlog 9,271 rows  | delay 15.6 s | missing 19
report 8:  read 15,676 | backlog 19,151 rows | delay 32.1 s | missing 19
report 11: read 21,676 | backlog 26,245 rows | delay 43.9 s | missing 19
stopped:   read 23,676 | backlog 24,605 rows | delay 47.7 s | missing 19
```

What happens to delay, backlog, and missing:
- Backlog and delay grow continuously. The detector processes one batch of 2,000 rows in ~7.4 s, which is ~270 readings/s (lower than the theoretical 333/s because of DB query and loop overhead). Since 600 readings/s are coming in, the backlog grows by ~330 rows/s and delay increases by ~4 s per batch. R3 (<= 5 s p95) fails after a few seconds. When stopped at 81 s, the detector had only processed up to t = 39 s of plant time, so it missed the fault at t = 40 s completely (0 anomalies).
- `missing` stops growing at 19. Those 19 rows were skipped during the first seconds when the detector was still polling near the live end of the table. Once it fell behind, it was querying old id ranges where all gateway transactions had already committed long ago. Falling behind hides the commit-order bug from A1 at the cost of breaking R3.


## Stage B - Decision record

Context:
The anomaly model takes 3 ms per reading (~333 readings/s theoretical max, ~270 readings/s actual) and keeps state per machine, requiring complete, in-order sequences (`seq`). Load is 600 readings/s in the lab and 100,000 readings/s in production. PostgreSQL remains the system of record for the machine registry.

Option 1: Keep PostgreSQL, poll better (multiple pollers, partitioned by machine subset)
- R1 (Scale): At risk. At 100,000 readings/s, PostgreSQL has to handle 100k inserts/s (~8.6 billion rows/day) plus hundreds of concurrent polling queries on the same table. Machine assignment across pollers has to be managed manually.
- R2 (Complete and ordered): Fails unless modified. Polling by `id` still suffers from out-of-order commits when pollers keep up with the head of the table. Fixing this requires polling per machine on `seq` or adding a time delay before reading, which hurts latency and adds query overhead.
- R3 (Fresh): Works at 600/s with enough pollers, but at heavy risk at 100k/s due to DB lock contention and I/O bottlenecks.
- R4 (Independent consumers): Fails. Adding a dashboard and training archive adds more read queries directly onto PostgreSQL, which violates R4.

Option 2: Gateways write to Kafka, detectors run as a consumer group
- R1 (Scale): Met. Topic partitions act as the unit of parallelism. Scaling out just means adding partitions and consumer instances.
- R2 (Complete and ordered): Met. Using `machine_id` as the message key hashes all readings of a machine to the same partition. Kafka appends to partitions sequentially and consumers read by contiguous offset, so nothing is skipped due to commit timing. Idempotent producers prevent duplicates or reordering on retries.
- R3 (Fresh): Met. Low-latency streaming; consumer lag can be monitored directly to scale instances.
- R4 (Independent consumers): Met. Archive and dashboard run as separate consumer groups with their own offsets. They read from Kafka without slowing down the detector or touching PostgreSQL.

Decision:
We choose Option 2 (Kafka). Gateways publish readings to a `readings` topic keyed by `machine_id`. Detector instances run in a single consumer group (`detector`) and publish alerts to the `anomalies` topic. PostgreSQL only stores the machine registry and telemetry.

Instance calculations and sizing:
- 600 readings/s (lab):
  Theoretical minimum: 600 / 333.3 = 1.8 -> 2 instances.
  Practical requirement: 1 instance actually manages ~270 readings/s (600 / 270 = 2.22 -> at least 3 instances). However, Kafka balances partitions across consumers, not individual keys. With murmur2 hashing across 6 partitions, the 60 machines distribute unevenly as 7, 18, 11, 9, 8, and 7 machines per partition. With 3 instances, one instance got assigned the 18-machine and 11-machine partitions (29 machines = 290 readings/s > 270/s capacity) and fell behind. Using 6 partitions and 6 instances (1 partition per instance) caps the busiest instance at 18 machines (180 readings/s), which stays well under 270/s.
- 100,000 readings/s (production):
  Theoretical minimum: 100,000 / 333.3 = 300 instances.
  Practical requirement: Accounting for the ~270 readings/s real throughput (100,000 / 270 = 371) and partition key skew, we need around 400-450 detector instances and at least as many partitions (e.g. 480-600 partitions across multiple brokers with replication factor 3). Partitions should be sized upfront so keys don't remap later.
- Tipping point:
  Database polling is fine for a small setup (like 1 reading/min) where a single writer, a single reader, and low throughput don't stress the DB. The tipping point is around a few hundred readings/s (~250-300 readings/s, where a single model process maxes out), or as soon as multiple concurrent writers cause commit-order gaps and multiple independent downstream consumers are needed. Our 600/s lab setup is already past that point.


## Stage C - Build the core

Topic creation commands (run before starting the plant so topics aren't auto-created with 1 partition):

```bash
docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --topic readings --partitions 6 --replication-factor 1
docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --topic anomalies --partitions 3 --replication-factor 1
```

Design notes:
- `KafkaSink` (`plant.py`): Sends messages keyed by `machine_id` with `acks=all`, `enable.idempotence=True`, `linger.ms=10`, `compression.type=lz4`, and `partitioner=murmur2_random`. Uses `poll(0)` on send and only calls `flush()` on shutdown so the plant does not block after every reading.
- `detector_service.py`: All instances join the same group (`detector`) so each partition is consumed by a single instance and machine sequences stay intact. On partition revocation (`on_revoke`), the service calls `model.forget()` for revoked machines and commits offsets synchronously so returning partitions warm up cleanly instead of comparing against stale state. Configured with `auto.offset.reset=latest` and manual offset storing (`enable.auto.offset.store=False`) after scoring.


### Run 1: 3 detector instances (fails R3 due to partition skew)

Scoreboard output:

```text
Run 1794156060: started 17:41:00 (3.8 min ago), 60 machines x 10 Hz, 4 faults so far

Detection  (topic anomalies: 1,117 anomaly events, models ['v1'])
   #  machine   start  detected   delay
   1  M-029       40s       yes   7.9 s
   2  M-017      100s       yes  12.2 s
   3  M-020      160s       yes  11.9 s
   4  M-036      220s  (just started, not scored yet)

  faults detected: 3/3, mean delay 10.7 s; false alarms (episodes on healthy machines): 0
  latency reading -> anomaly event: p50 15.11 s, p95 22.57 s, max 23.37 s
  duplicate anomaly events (same anomaly_id): 0

Detector instances  (model telemetry, this run)
  det-1          pid 1752    v1  readings    33,914  missing       0  out-of-order       0  machines   15  alive
  det-2          pid 34640   v1  readings    58,961  missing       0  out-of-order       0  machines   29  alive
  det-3          pid 33620   v1  readings    36,175  missing       0  out-of-order       0  machines   16  alive

Requirements (measurable part)
  [PASS] R2 complete and ordered: 0 readings missing, 0 out of order
  [FAIL] R3 fresh: p95 latency < 5 s
  [PASS] detection: all faults found, at most 2 false alarms
```

Explanation of Run 1:
R2 and fault detection passed with 0 missing and 0 out-of-order readings. R3 failed (p95 latency 22.57 s) because each instance got 2 partitions, and `det-2` was assigned partitions 1 and 2, which hashed 18 + 11 = 29 machines (290 readings/s). Because 290/s exceeds the ~270/s single-process capacity, `det-2` accumulated over 8,000 messages of lag while `det-1` (15 machines) and `det-3` (16 machines) stayed caught up.


### Run 2: 6 detector instances (all PASS)

Commands run:

```bash
python detector_service.py --name det-1  # through det-6 in separate terminals
python plant.py --sink kafka
python scoreboard.py
```

Scoreboard output:

```text
Run 1794158413: started 18:20:13 (7.9 min ago), 60 machines x 10 Hz, 4 faults so far

Detection  (topic anomalies: 1,587 anomaly events, models ['v1'])
   #  machine   start  detected   delay
   1  M-029       40s       yes   7.9 s
   2  M-026      100s       yes   8.2 s
   3  M-038      160s       yes   7.8 s
   4  M-001      220s       yes   6.8 s

  faults detected: 4/4, mean delay 7.7 s; false alarms (episodes on healthy machines): 0
  latency reading -> anomaly event: p50 0.08 s, p95 0.09 s, max 0.17 s
  duplicate anomaly events (same anomaly_id): 0

Detector instances  (model telemetry, this run)
  det-1          pid 19404   v1  readings    18,279  missing       0  out-of-order       0  machines    7  silent for 213 s
  det-2          pid 25500   v1  readings    28,697  missing       0  out-of-order       0  machines   11  silent for 213 s
  det-3          pid 32936   v1  readings    23,496  missing       0  out-of-order       0  machines    9  silent for 213 s
  det-4          pid 2532    v1  readings    46,940  missing       0  out-of-order       0  machines   18  silent for 213 s
  det-5          pid 39832   v1  readings    18,278  missing       0  out