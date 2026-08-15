from confluent_kafka import Producer
import sys

def main():
    conf = {
        'bootstrap.servers' : 'localhost:9092',
        'client.id': 'python-producer'
    }

    try:
        producer = Producer(conf)
        print("Producer created")
    except Exception as e:
        print(f"Failed to create producer: {e}")
        sys.exit(1)

    topic = 'test-topic'

    test_data = [
        {"key": 'key1', "value":"value1"},
        {"key": 'key2', "value":"value2"},
        {"key": 'key3', "value":"value3"}
    ]

    print("Sending messages to topic")
    for item in test_data:
        producer.produce(
            topic= topic,
            key= item["key"].encode(),
            value= item["value"].encode()
        )

    print("Flushing messages")
    producer.flush()

if __name__ == '__main__':
    main()