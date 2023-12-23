import json
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from pyhive import hive
from datetime import datetime, timedelta




# Kafka configuration
kafka_bootstrap_servers = 'kafka:9092'
kafka_topic = 'batch_topic'

# Avro schema for your data
avro_schema = {
    "type": "record",
    "name": "TradeRecord",
    "fields": [
        {"name": "p", "type": "double"},
        {"name": "exchange", "type": "string"},
        {"name": "crypto_pair", "type": "string"},
        {"name": "t", "type": "long"},
        {"name": "v", "type": "double"}
    ]
}

# Hive configuration
hive_host = 'hive-server'
hive_port = 10000
hive_database = 'finnhub_db'
hive_table_name = 'finnhub_table'
avro_schema_string = json.dumps(avro_schema)


def extract_trade_objects(avro_data_json):
    trade_objects = avro_data_json.get("data", [])
    return trade_objects


def create_temporary_table(cursor, temp_table_name, avro_schema_string):
    create_temp_table_query = f"""
        CREATE TABLE IF NOT EXISTS {temp_table_name}
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
        STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
        TBLPROPERTIES ('avro.schema.literal'='{avro_schema_string}')
    """
    cursor.execute(create_temp_table_query)


def create_hive_table():
    connection = hive.connect(host=hive_host, port=hive_port, database=hive_database)
    cursor = connection.cursor()

    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {hive_table_name}
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
        STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
        TBLPROPERTIES ('avro.schema.literal'='{avro_schema_string}')
    """

    cursor.execute(create_table_query)
    connection.close()


def process_and_insert_into_hive(trade_objects, cursor, temp_table_name):
    if not trade_objects:
        return

    try:
        # Insert trade objects into the temporary table
        print(trade_objects)
        for trade_object in trade_objects:
            # Check if trade_object is a dictionary

            for trade in trade_object:
                # print("\n trade object")
                # print(trade)

                # Load the JSON string into a Python dictionary
                avro_record_json = json.dumps(trade)
                json_dict = json.loads(avro_record_json)
                # Extract exchange and crypto_pair from 's'
                exchange, crypto_pair = json_dict['s'].split(':') if ':' in json_dict['s'] else (json_dict['s'], None)

                # Handle the 'c' attribute, converting None to a string representation
                '''c_value = json_dict['c']
                print(c_value)'''

                cursor.execute(
                    f"""
                            INSERT INTO {temp_table_name}
                            VALUES (

                                {json_dict['p']},
                                '{exchange}',  -- New 'exchange' attribute
                                '{crypto_pair}',  -- New 'crypto_pair' attribute
                                {json_dict['t']},
                                {json_dict['v']}
                            )
                        """)

        # Insert data from the temporary table into the main table
        insert_query = f"""
            INSERT INTO TABLE {hive_table_name} 
            SELECT *
            FROM {temp_table_name}
        """

        print("\nInserting batch into Hive...")

        cursor.execute(insert_query)
        print("Batch successfully inserted into Hive.")

        # Drop the temporary table
        # cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")

    except Exception as e:
        print(f"Error processing and inserting into Hive: {e}")


def kafka_consumer_worker():
    create_hive_table()

    kafka_consumer_conf = {
        'bootstrap.servers': kafka_bootstrap_servers,
        'group.id': 'finnhub',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(kafka_consumer_conf)
    consumer.subscribe([kafka_topic])

    batch_size = 10  # Adjust as needed
    avro_data_batch = []

    connection = hive.connect(host=hive_host, port=hive_port, database=hive_database)
    cursor = connection.cursor()

    temp_table_name = 'temp_avro_data'
    create_temporary_table(cursor, temp_table_name, avro_schema_string)
    # Get the current time when the consumer starts
    messages_consumed = 0


    while messages_consumed < 2:
        msg = consumer.poll(1000)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Error: {msg.error()}")
                break

        try:
            # Extract Avro data from the JSON structure
            avro_data_json = json.loads(msg.value())
            avro_data = extract_trade_objects(avro_data_json)

            if avro_data is not None:
                avro_data_batch.append(avro_data)
                messages_consumed += 1  # Increment the message count

                if len(avro_data_batch) >= batch_size:
                    process_and_insert_into_hive(avro_data_batch, cursor, temp_table_name)
                    avro_data_batch = []



        except Exception as e:
            print(f"Error processing Avro data: {e}")
            print(f"Avro data: {msg.value().decode('utf-8')}")  # Print Avro data as a string
    print("finish")
    connection.close()


if __name__ == "__main__":
    kafka_consumer_worker()