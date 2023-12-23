from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Connection
from airflow.utils import db
import json
from confluent_kafka import Producer, Consumer, KafkaError


def kafka_consumer_worker():
    kafka_consumer_conf = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'my_consumer_group',
        'auto.offset.reset': 'earliest',
    }

    # Kafka topic to consume from
    kafka_topic = 'finnhub_topic'
    consumer = Consumer(kafka_consumer_conf)
    consumer.subscribe([kafka_topic])

    for i in range(1,100):
        msg = consumer.poll(timeout=1000)  # Poll for messages every 1 second

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                print(f"Reached end of partition, offset: {msg.offset()}")
            else:
                print(f"Error: {msg.error()}")
        else:
            # Process the received message
            print(f"Received message: {msg.value().decode('utf-8')}")
# [END import_module]
def kafkaConsumer():
    kafka_topic = 'finnhub_topic'
    kafka_producer_conf = {
        'bootstrap.servers': 'kafka:9092'
    }
    kafka_producer = Producer(kafka_producer_conf)



    for i in range(1,100):
        print("hamza")
        kafka_producer.produce(topic=kafka_topic, value=json.dumps({'data': [{'p': 2256.73, 's': 'BINANCE:ETHUSDT', 't': 1702744544300, 'v': 0.0241}, {'p': 42632.75, 's': 'BINANCE:BTCUSDT', 't': 1702744544455, 'v': 0.33695}, {'p': 42632.75, 's': 'BINANCE:BTCUSDT', 't': 1702744544455, 'v': 0.16305}, {'p': 42632.75, 's': 'BINANCE:BTCUSDT', 't': 1702744544491, 'v': 0.00117}, {'p': 42632.75, 's': 'BINANCE:BTCUSDT', 't': 1702744544619, 'v': 0.01351}], 'type': 'trade'}))
        kafka_producer.poll(0)
        kafka_producer.flush()




# def producer_function():
#     for i in range(20):
#         yield (json.dumps(i), json.dumps(i + 1))
# # db.merge_conn(
#         Connection(
#             conn_id="t3_3",
#             conn_type="kafka",
#             extra=json.dumps(
#                 {
#                     "bootstrap.servers": "kafka:9092",
#                     "group.id": "t232",
#                     "enable.auto.commit": False,
#                     "auto.offset.reset": "beginning",
#                 }
#             ),
#         )
#     )
# db.merge_conn(
#         Connection(
#             conn_id="t1-3",
#             conn_type="kafka",
#             extra=json.dumps({"socket.timeout.ms": 10, "bootstrap.servers": "kafka:9092"}),
#         )
#     )
# [START instantiate_dag]
with DAG(
    "batch",
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'trigger_rule': 'all_success'
    },
    # [END default_args]
    description="A simple tutorial DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    # [END instantiate_dag]

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    # [START basic_task]
    # t1 = ProduceToTopicOperator(
    #     kafka_config_id="t1-3",
    #     task_id="produce_to_topic",
    #     topic="finnhub_topic",
    #     producer_function="batchLayer.producer_function",
    # )
    # t2 = ConsumeFromTopicOperator(
    #     task_id='consume_kafka_messages',
    #     topics=['finnhub_topic'],  # Kafka topic to consume messages from
    #     kafka_config_id="t3_3",
    #     apply_function="batchLayer.process_kafka_message",  # Callback function to process each message
    #
    # )

    t1 = PythonOperator(
        task_id="print_date",
        python_callable=kafkaConsumer,
    )
    # t2 = PythonOperator(
    #     task_id="print",
    #     python_callable=kafka_consumer_worker,
    # )

    # [END basic_task]



    # t1
    # t1 >> t2
# [END tutorial]