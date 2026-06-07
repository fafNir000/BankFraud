import time
import json
import pandas as pd
import uuid
from confluent_kafka import Producer

kafka_config = {
    'bootstrap.servers': 'kafka:29092',
    'client.id': 'paysim_transaction_generator'
}

producer = Producer(kafka_config)
TOPIC_NAME = 'transactions'

def delivery_report(err, msg):
    if err is not None:
        print(f"Message Error :: {err}")

def generate_stream(csv_path):
    print(f"Reading dataset :: {csv_path}")
    print("Pay Simulator Generator: Sending transactions to Kafka")
    try:
        while True:
            for chunk in pd.read_csv(csv_path, chunksize=500):
                chunk = chunk.drop(['isFraud', 'isFlaggedFraud'], axis=1)
                
                for index, row in chunk.iterrows():
                    transaction_data = row.to_dict()
                    
                    transaction_data['timestamp'] = int(time.time())
                    transaction_data['transaction_id'] = str(uuid.uuid4())

                    payload = json.dumps(transaction_data).encode('utf-8')

                    producer.produce(topic=TOPIC_NAME, value=payload, callback=delivery_report)
                    producer.poll(0)
                    
                    print(f"[{transaction_data['type']}] ID #{index}: {transaction_data['amount']} USD")
                    time.sleep(0.3)
                    
    except KeyboardInterrupt:
        print("\nGenerator Stopped")
    finally:
        producer.flush()

if __name__ == "__main__":
    CSV_FILE_PATH = "data/data.csv"
    generate_stream(CSV_FILE_PATH)