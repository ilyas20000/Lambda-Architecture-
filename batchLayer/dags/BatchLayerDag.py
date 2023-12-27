from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
import json
from confluent_kafka import Producer, Consumer, KafkaError
import batch_consumer as hive_consumer
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Connection
from airflow.utils import db
import batch_proc_0 as b0
import batch_proc_1 as b1
import batch_proc_2 as b2
import batch_proc_3 as b3





                          
def save_data_to_hive():
    hive_consumer.kafka_consumer_worker()

def batch_0():
    b0.batch_proc_0()
def batch_1():
    b1.batch_proc_1()
def batch_2():
    b2.batch_proc_2()
def batch_3():
    b3.batch_proc_3()



with DAG( "batch_layer", default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="...",
    schedule_interval="@monthly",

    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["batch layer, lambda"],
) as dag:

    t1 = PythonOperator(
        task_id="consume_data",
        python_callable=save_data_to_hive,
    )

    t2 = PythonOperator(
        task_id="batch_0",
        python_callable=batch_0,
    )

    t3 = PythonOperator(
        task_id="batch_1",
        python_callable=batch_1,
    )
    t4 = PythonOperator(
        task_id="batch_2",
        python_callable=batch_2,
    )
    t5 = PythonOperator(
        task_id="batch_3",
        python_callable=batch_3,
    )



    t1 >> [t2,t3,t4,t5]
