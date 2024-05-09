from confluent_kafka import Producer, Consumer
import time
import threading

# Kafka broker configurations
bootstrap_servers = 'localhost:9092'
group_id = 'test-group'

# Kafka topic
topic = 'test-topic'

# Function to produce messages
def produce_messages():
    p = Producer({'bootstrap.servers': bootstrap_servers})

    try:
        for i in range(10):
            message = f"Message {i}"
            p.produce(topic, message.encode('utf-8'))
            print(f"Produced: {message}")
            time.sleep(1)
        p.flush()
    except KeyboardInterrupt:
        pass

# Function to consume messages
def consume_messages():
    c = Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    })
    c.subscribe([topic])

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            print(f"Consumed: {msg.value().decode('utf-8')}")
    except KeyboardInterrupt:
        pass
    finally:
        c.close()

if __name__ == "__main__":
    # Produce messages in a separate thread
    produce_thread = threading.Thread(target=produce_messages)
    produce_thread.start()

    # Consume messages
    consume_messages()
