import json
import requests
import threading
import time
import socket

app = Flask(__name__)

JSON_URL = "https://api.apify.com/v2/datasets/0W1msdVfrNKSJyEDQ/items?token=apify_api_xWO8WuBaVKxXGyyDXxknjceLVVwLgn1AYTDv"
JSON_FILE = "data.json"
BATCH_SIZE = 5
NETCAT_HOST = "localhost"
NETCAT_PORT = 12345

def fetch_and_store_json():
    response = requests.get(JSON_URL)
    data = response.json()
    with open(JSON_FILE, "w") as file:
        json.dump(data, file)

def send_json_to_netcat():
    with open(JSON_FILE, "r") as file:
        data = json.load(file)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((NETCAT_HOST, NETCAT_PORT))

    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i+BATCH_SIZE]
        batch_json = json.dumps(batch)
        sock.sendall(batch_json.encode())
        time.sleep(100)  # Delay between sending batches

    sock.close()

@app.route("/")
def index():
    fetch_and_store_json()
    return "JSON data fetched and stored. Sending to netcat port."

if __name__ == "__main__":
    fetch_and_store_json()
    threading.Thread(target=send_json_to_netcat).start()
    app.run()