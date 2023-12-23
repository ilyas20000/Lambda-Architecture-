from __future__ import annotations
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

def parquet():
    try:
        # Use the open function with 'x' mode to create the file if it doesn't exist
        with open('/tmp/batchViews/df.parquet', 'x'):
            pass  # This block is empty, creating an empty file

        print(f"File '/tmp/batchViews/df.parquet' created successfully.")
    except FileExistsError:
        print(f"File '/tmp/batchViews/df.parquet' already exists.")
    #create the schema
    schema = pa.schema([
        ('column1', pa.int64()),
        ('column2', pa.string()),
        # Add more columns as needed
    ])
    data = [
        {'column1': 1, 'column2': 'value1'},
        {'column1': 2, 'column2': 'value2'},
        # Add more rows as needed
    ]

    #create the table
    table = pa.Table.from_pandas(pd.DataFrame(data), schema=schema)
    pq.write_table(table, '/tmp/batchViews/df.parquet')

    #read data
    df = pq.read_pandas('/tmp/batchViews/df.parquet').to_pandas()

    print(df)


with DAG(
    "parquet",
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
    },
    # [END default_args]
    description="A simple tutorial DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    # [END instantiate_dag]



    t1 = PythonOperator(
        task_id="parquet",
        python_callable=parquet,
    )


    # [END basic_task]




    t1
# [END tutorial]