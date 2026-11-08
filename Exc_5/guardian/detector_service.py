"""The anomaly detector as a Kafka service. YOUR CODE (Part 2, Stage C2).

The plumbing is here, the decisions (marked TODO) are yours. Contract with
the maintenance team: every reading goes through model.score(); every
reading with score >= model.threshold becomes ONE anomaly event,
model.anomaly_event(reading, score), as JSON, published to the topic
`anomalies`.

    python3 detector_service.py --name det-1
    python3 detector_service.py --name det-2          # one more instance

Decisions (reasons in SOLUTION.md):
  - in-topic `readings` (key = machine_id), group `detector`: all instances
    share one group, so every partition, and with it every machine, belongs
    to exactly one instance.
  - cooperative-sticky assignment: when an instance joins or leaves, only the
    partitions that really move are revoked; the others keep their model state.
  - on revoke: forget the model state of the machines in the revoked
    partitions (it would be stale if the partition comes back later) and
    commit what was processed, so the next owner continues right there.
  - offsets are stored only AFTER a reading was scored (at-least-once).
  - anomalies are keyed by machine_id: one machine's anomalies stay in order.
"""
import argparse
import json
import time
from collections import defaultdict

from confluent_kafka import Consumer, KafkaException, Producer

from model import BearingModel

ap = argparse.ArgumentParser()
ap.add_argument("--name", default="det-1", help="instance name (shows up in the scoreboard)")
ap.add_argument("--in-topic", default="readings", help="the topic the gateways write to")
ap.add_argument("--group", default="detector", help="the consumer group of this detector")
ap.add_argument("--out-topic", default="anomalies")
ap.add_argument("--bootstrap", default="localhost:9092")
args = ap.parse_args()

model = BearingModel(instance=args.name)
consumer = Consumer({
    "bootstrap.servers": args.bootstrap,
    "group.id": args.group,
    "client.id": args.name,
    # Only used when the group has NO committed offset for a partition (first start).
    # A live detector must alert within seconds (R3): replaying up to 7 days of
    # history first would make every alert hours late. A new archive consumer
    # would choose "earliest" instead.
    "auto.offset.reset": "latest",
    "partition.assignment.strategy": "cooperative-sticky",
    # commit automatically, but only offsets we stored after scoring (at-least-once)
    "enable.auto.commit": True,
    "enable.auto.offset.store": False,
})
producer = Producer({
    "bootstrap.servers": args.bootstrap,
    "acks": "all",
    "enable.idempotence": True,
    "partitioner": "murmur2_random",
    "linger.ms": 5,
})

machines_in = defaultdict(set)      # partition -> machines this instance has seen in it


def on_assign(c, partitions):
    print(f"[{args.name}] assigned partitions {sorted(p.partition for p in partitions)}", flush=True)


def on_revoke(c, partitions):
    """A partition moves to another instance: drop the model state of its machines."""
    for p in partitions:
        for m in machines_in.pop(p.partition, ()):
            model.forget(m)
    try:
        c.commit(asynchronous=False)            # hand over exactly where we stopped
    except KafkaException:
        pass                                    # nothing stored yet
    print(f"[{args.name}] revoked partitions {sorted(p.partition for p in partitions)}, "
          f"now keeping state for {len(model.machines())} machines", flush=True)


consumer.subscribe([args.in_topic], on_assign=on_assign, on_revoke=on_revoke, on_lost=on_revoke)
print(f"[{args.name}] {args.in_topic} -> {args.out_topic}, group {args.group} (Ctrl+C to stop)", flush=True)

readings = anomalies = 0
report_at = time.time() + 5
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"[{args.name}] {msg.error()}")
            continue
        reading = json.loads(msg.value())
        machines_in[msg.partition()].add(reading["machine_id"])
        score = model.score(reading)
        readings += 1
        if score is not None and score >= model.threshold:
            anomalies += 1
            event = model.anomaly_event(reading, score)
            producer.produce(args.out_topic, key=reading["machine_id"],   # per-machine order
                             value=json.dumps(event))
        producer.poll(0)
        consumer.store_offsets(msg)             # processed -> may be committed
        if time.time() >= report_at:
            lag = 0                             # how far behind the end of the log (all my partitions)
            for tp in consumer.assignment():
                hi = consumer.get_watermark_offsets(tp, cached=True)
                pos = consumer.position([tp])[0].offset
                if hi and hi[1] >= 0 and pos >= 0:
                    lag += hi[1] - pos
            print(f"[{args.name}] {readings:>9,} readings  {anomalies:>6,} anomalies  "
                  f"lag {lag:>6,}  delay {time.time() - reading['ts']:5.2f} s  "
                  f"missing {model.missing}  out-of-order {model.out_of_order}  "
                  f"machines {len(model.machines())}", flush=True)
            report_at = time.time() + 5         # (+= 5 printed every reading after an idle start)
except KeyboardInterrupt:
    pass
finally:
    producer.flush(10)
    consumer.close()                            # commits stored offsets, leaves the group cleanly
    print(f"[{args.name}] stopped after {readings:,} readings, {anomalies:,} anomalies")
