import time
import json
import pandas as pd
from confluent_kafka import Producer

kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'paysim_transaction_generator'
}

producer = Producer(kafka_config)
TOPIC_NAME = 'transactions'

def delivery_report(err, msg):
    if err is not None:
        print(f"Message Error :: {err}")

def generate_stream(csv_path):
    print(f"Reading dataset :: {csv_path}")
    df = pd.read_csv(csv_path)   
    df = df.drop(['isFraud', 'isFlaggedFraud'], axis=1)
    print("Pay Simulator Generator: Sending transactions to Kafka")
    
    try:
        while True:
            for index, row in df.iterrows():
                transaction_data = row.to_dict()
                
                transaction_data['timestamp'] = int(time.time())
                transaction_data['transaction_id'] = index

                payload = json.dumps(transaction_data).encode('utf-8')

                producer.produce(topic=TOPIC_NAME, value=payload, callback=delivery_report)
                producer.poll(0)
                print(f"[{transaction_data['type']}] ID #{index}: {transaction_data['amount']} USD")
                time.sleep(0.3)
                
    except KeyboardInterrupt:
        print("Generator Stopped")
    finally:
        producer.flush()

if __name__ == "__main__":
    CSV_FILE_PATH = "data/card_transdata.csv"
    generate_stream(CSV_FILE_PATH)