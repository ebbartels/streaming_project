from confluent_kafka import Consumer
import sys

def main():

    conf = {
            'bootstrap.servers' : 'localhost:9092',
            'group.id':'my-python-consumer-group',
            'auto.offset.reset': 'earliest'
        }

    try:
        consumer = Consumer(conf)
        print("Consumer created")
    except Exception as e:
        print(f"Failed to create consumer: {e}")
        sys.exit(1)

    topic = 'train-positions'
    consumer.subscribe([topic])
    print(f'Consumer subscribed to {topic}')

    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue

        elif msg.error():

            print(msg.error())

        else:
            key = msg.key().decode() if msg.key() else None
            value = msg.value().decode() if msg.value() else None
            print(f"Received message: Key={key}, Value={value} | "
                      f"Topic={msg.topic()}, Partition={msg.partition()}, Offset={msg.offset()}")



if __name__ == '__main__':
    main()