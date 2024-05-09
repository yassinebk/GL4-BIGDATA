import json
from confluent_kafka import Producer
import requests
import threading
import time
import socket
from flask import Flask

app = Flask(__name__)

JSON_URL = "https://api.apify.com/v2/datasets/0W1msdVfrNKSJyEDQ/items?token=apify_api_xWO8WuBaVKxXGyyDXxknjceLVVwLgn1AYTDv"
JSON_FILE = "data.json"
BATCH_SIZE = 5

producer = Producer({'bootstrap.servers': 'localhost:9092'})

def fetch_and_store_json():
    response = requests.get(JSON_URL)
    data = response.json()
    with open(JSON_FILE, "w") as file:
        json.dump(data, file)

def send_data_to_kafka():
    with open('data.json', 'r') as file:
        data = json.load(file)

    batch_size = 5
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        producer.produce('job_data_topic', json.dumps(batch))
        print('[+] Sending new data ....')
        time.sleep(10)  # Delay between sending batches

@app.route("/")
def index():
    fetch_and_store_json()
    return "JSON data fetched and stored. Sending to netcat port."

if __name__ == "__main__":
    fetch_and_store_json()
    threading.Thread(target=send_data_to_kafka).start()
    app.run()