import json
import random
import subprocess
from datetime import datetime, timedelta

topic = "orders_raw"
bootstrap = "localhost:9092"

statuses = ["CREATED", "PAID", "SHIPPING", "DELIVERED", "CANCELLED"]
base_time = datetime(2026, 5, 27, 15, 0, 0)

cmd = [
    "kafka-console-producer.sh",
    "--bootstrap-server", bootstrap,
    "--topic", topic
]

p = subprocess.Popen(cmd, stdin=subprocess.PIPE, text=True)

for i in range(1, 1001):
    order_id = 100000 + i
    user_id = f"user-{random.randint(1, 100)}"
    amount = random.randint(1000, 300000)
    status = random.choice(statuses)
    event_time = base_time + timedelta(seconds=i)

    event = {
        "event_id": f"bulk-{i:06d}",
        "op": "u" if status != "CREATED" else "c",
        "order_id": order_id,
        "user_id": user_id,
        "amount": amount,
        "status": status,
        "event_time": event_time.strftime("%Y-%m-%dT%H:%M:%S")
    }

    p.stdin.write(json.dumps(event) + "\n")

p.stdin.close()
p.wait()

print("Produced 1000 order events.")
