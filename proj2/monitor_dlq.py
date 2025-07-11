from confluent_kafka import Consumer
import json

dlq_consumer = Consumer({
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'dlq-monitor',
    'auto.offset.reset': 'earliest'
})

dlq_consumer.subscribe(['bf_employee_cdc_dlq'])

while True:
    msg = dlq_consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"DLQ Error: {msg.error()}")
    else:
        record = json.loads(msg.value())
        print("DLQ Message:")
        print(json.dumps(record, indent=2))
