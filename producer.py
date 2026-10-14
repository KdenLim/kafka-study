from confluent_kafka import Producer
import json

producer = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Great success! {msg.value().decode('utf-8')}")
        print(f"Delivered to {msg.topic()} topic, partition {msg.partition()} at offset {msg.offset()}")

friends = {
    "friend_id": "fwen1",
    "name": "Bob Ross",
    "hobby": "Paint"
}

friend_value = json.dumps(friends).encode("utf-8")

producer.produce(topic="friends", value=friend_value, callback=delivery_report)

producer.flush()