import requests, os, sys, time, json
from dotenv import load_dotenv
from confluent_kafka import Producer

def main():

    conf = {
        'bootstrap.servers' : 'localhost:9092',
        'client.id': 'python-producer'
    }
    load_dotenv()

    api_key = os.getenv('WMATA_API_KEY')

    url = 'https://api.wmata.com/TrainPositions/TrainPositions?contentType=json'
    headers = {'api_key':api_key}
    

    try:
        producer = Producer(conf)
        print("Producer created")
    except Exception as e:
        print(f"Failed to create producer: {e}")
        sys.exit(1)

    topic = 'train-positions'

    while True:
        time.sleep(10)
        try:
            response = requests.get(url=url, headers=headers, timeout=5)
            if response.status_code != 200: print(response.status_code)
            train_data = response.json()

            print("Sending messages to topic")
            for train_info in train_data['TrainPositions']:
                try:
                    producer.produce(
                        topic= topic,
                        key= train_info["TrainId"].encode(),
                        value= json.dumps(train_info).encode()
                    )
                except KeyError as e:
                    print("Missing TrainId Key")
                    print('Skipping and moving on')
                    print(e)
                    continue

            print("Flushing messages")
            producer.flush()

        except requests.exceptions.RequestException as e:
            print("Request error")
            
            print(e)
            continue
        except Exception as e:
            print("Failed to read and send train data")
            print(e)
            continue


if __name__ == '__main__':
    main()