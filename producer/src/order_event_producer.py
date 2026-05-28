import json
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

import yaml
from confluent_kafka import Producer


EVENT_STEPS = [
    ("ORDER_CREATED", "c", "CREATED"),
    ("PAYMENT_COMPLETED", "u", "PAID"),
    ("SHIPMENT_STARTED", "u", "SHIPPING"),
    ("SHIPMENT_DELIVERED", "u", "DELIVERED"),
]


def load_config():
    config_path = Path(__file__).resolve().parents[1] / "config" / "local.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] delivery failed: {err}")
    else:
        print(
            f"[INFO] delivered topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )


def build_event(order_id, user_id, amount, event_type, op, status, event_time):
    return {
        "event_id": f"ord-{order_id}-{event_type.lower()}",
        "event_type": event_type,
        "op": op,
        "order_id": order_id,
        "user_id": user_id,
        "amount": amount,
        "status": status,
        "event_time": event_time.strftime("%Y-%m-%dT%H:%M:%S"),
    }


def main():
    config = load_config()

    producer = Producer({
        "bootstrap.servers": config["kafka"]["bootstrap_servers"]
    })

    topic = config["kafka"]["topic"]
    total_orders = config["producer"]["total_orders"]
    interval = config["producer"]["interval_seconds"]

    base_time = datetime.now().replace(microsecond=0)

    for i in range(1, total_orders + 1):
        order_id = 202605270000 + i
        user_id = f"cust-{random.randint(10000, 99999)}"
        amount = random.randint(5000, 300000)

        for step, (event_type, op, status) in enumerate(EVENT_STEPS):
            event_time = base_time + timedelta(seconds=i * 10 + step)

            event = build_event(
                order_id=order_id,
                user_id=user_id,
                amount=amount,
                event_type=event_type,
                op=op,
                status=status,
                event_time=event_time,
            )

            producer.produce(
                topic=topic,
                key=str(order_id),
                value=json.dumps(event, ensure_ascii=False),
                callback=delivery_report,
            )

            producer.poll(0)
            time.sleep(interval)

    producer.flush()
    print("[INFO] finished producing order lifecycle events")


if __name__ == "__main__":
    main()
