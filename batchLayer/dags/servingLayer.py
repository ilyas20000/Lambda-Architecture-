import os
import psycopg2
from psycopg2 import sql
from pyarrow import parquet
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
# Database connection parameters
db_params = {
    'host': 'postgres',
    'database': 'airflow',
    'user': 'airflow',
    'password': 'airflow',
    'port': '5432'
}

# Directory containing Parquet files
parquet_directory = '/tmp/batchViews'


def create_table(cursor):
    # Define your table schema here
    table_creation_query = """
    CREATE TABLE IF NOT EXISTS your_table (
        id SERIAL PRIMARY KEY,
        column1 VARCHAR,
        column2 VARCHAR
        -- Add more columns as needed
    );
    """
    cursor.execute(table_creation_query)


def insert_parquet_data(cursor, file_path):
    # Read Parquet file and insert data into the table
    parquet_data = parquet.read_table(file_path).to_pandas()

    insert_query = sql.SQL("""
        INSERT INTO your_table (column1, column2)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING;
    """)

    for index, row in parquet_data.iterrows():
        cursor.execute(insert_query, (row['column1'], row['column2']))


def process_parquet_files():
    try:
        # Connect to the PostgreSQL database
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        # Create the table if it doesn't exist
        create_table(cursor)

        # Loop through Parquet files in the directory
        for file_name in os.listdir(parquet_directory):
            if file_name.endswith(".parquet") and "-inserted" not in file_name:
                file_path = os.path.join(parquet_directory, file_name)

                # Insert data into the database
                insert_parquet_data(cursor, file_path)

                # Rename the file by appending "-inserted"
                new_file_name = file_name.replace(".parquet", "-inserted.parquet")
                os.rename(file_path, os.path.join(parquet_directory, new_file_name))

        # Commit the transaction
        connection.commit()
        print("Data inserted successfully!")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()


with DAG(
    "serving-layer",
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
        task_id="serving",
        python_callable=process_parquet_files,
    )

