import json
import threading
import queue
import websocket
from confluent_kafka import Producer
from confluent_kafka import Consumer

# Kafka topic to which you want to send messages
kafka_topic1 = 'speed_topic'
kafka_topic2 = 'batch_topic'

# Queue for passing messages from WebSocket thread to Kafka thread
message_queue = queue.Queue()

def kafka_producer_worker():
    kafka_producer_conf = {
        'bootstrap.servers': 'kafka',
        'acks': 'all',
    }
    kafka_producer = Producer(kafka_producer_conf)


    while True:
        message = message_queue.get()
        print(f"Sending message to Kafka: {message}")
        try:
            kafka_producer.produce(topic=kafka_topic1, value=json.dumps(message))
            kafka_producer.produce(topic=kafka_topic2, value=json.dumps(message))
            kafka_producer.poll(0)
            kafka_producer.flush()

        except Exception as e:
            print(f"Error sending message to Kafka: {e}")

        print("Message sent to Kafka")



def on_message(ws, message):
    try:
        data = json.loads(message)
        message_queue.put(data)
    except json.JSONDecodeError as e:
        print(f"JSON decoding error: {e}")
    except Exception as e:
        print(f"Error handling message: {e}")

def on_error(ws, error):
    print(f"WebSocket error: {error}")

def on_close(ws):
    print("WebSocket closed")

def on_open(ws):
    ws.send('{"type":"subscribe","symbol":"AAPL"}')
    ws.send('{"type":"subscribe","symbol":"AMZN"}')
    ws.send('{"type":"subscribe","symbol":"MSFT"}')
    ws.send('{"type":"subscribe","symbol":"GBP"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:ETHUSDT"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')


if __name__ == "__main__":
    # Start Kafka producer worker thread
    kafka_thread = threading.Thread(target=kafka_producer_worker, daemon=True)
    kafka_thread.start()

    # WebSocket configuration
    ws = websocket.WebSocketApp(
        "wss://ws.finnhub.io?token=clnmfh9r01qqp7jp4asgclnmfh9r01qqp7jp4at0",
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.on_open = on_open
    ws.run_forever()
