import json
from confluent_kafka import Consumer

consumer = Consumer({'bootstrap.servers': 'localhost:9092',
                    'group.id': 'consumer-group',
                    'auto.offset.reset': 'earliest'})

consumer.subscribe(['friends'])

print('consumer is running')

try:
    while True:
        msg = consumer.poll(1)
        if msg is None:
            continue
        if msg.error():
            print(msg.error())
            continue

        value = msg.value().decode('utf-8')
        friend = json.loads(value)
        print(f"friend {friend['name']} who likes {friend['hobby']}")
except KeyboardInterrupt:
    print("\n Stopping Consumer")
finally:
    consumer.close()

