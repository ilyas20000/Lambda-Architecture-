from datetime import datetime, timedelta
import pandas as pd
from pyhive import hive
import pyarrow as pa
import pyarrow.parquet as pq

def batch_proc_0():
    # Hive configuration
    hive_host = 'hive-server'
    hive_port = 10000
    hive_database = 'finnhub_db'
    hive_table_name = 'finnhub_table'
    data = []

    connection = hive.connect(host=hive_host, port=hive_port, database=hive_database)
    cursor = connection.cursor()
    current_timestamp = int(datetime.now().timestamp() * 1000)  # Conv
    


    get_query = f"""
                SELECT *
                FROM {hive_table_name}
            """

    cursor.execute(get_query)
    for row in cursor.fetchall():
        result_dict = {'p': row[0], 's': row[1], 't': str(datetime.fromtimestamp(round(row[3]/1000.0))), 'v': row[4]}
        data.append(result_dict)

    
    # Create a Pandas DataFrame
    df = pd.DataFrame(data)

    # Convert the 't' column to a Pandas datetime object
    df['t'] = pd.to_datetime(df['t'])

    # Extract date and hour from the 't' column
    df['date'] = df['t'].dt.date
    df['hour'] = df['t'].dt.hour

    # Group by both date and hour, then calculate the sum of 'v'
    result = df.groupby(['date', 'hour', 's']).agg(sum_v=('v', 'sum')).reset_index()

    # Show the result
    print(result)

    try:
        # Use the open function with 'x' mode to create the file if it doesn't exist
        with open('/tmp/batchViews/parquet_output.parquet', 'x'):
            pass  # This block is empty, creating an empty file

        print(f"File '/tmp/batchViews/parquet_output.parquet' created successfully.")
    except FileExistsError:
        print(f"File '/tmp/batchViews/parquet_output.parquet' already exists.")

    # Write the result to a Parquet file
    parquet_file_path = '/tmp/batchViews/parquet_output.parquet'
    table = pa.Table.from_pandas(result)
    pq.write_table(table, parquet_file_path)

    #read data
    df = pq.read_pandas('/tmp/batchViews/parquet_output.parquet').to_pandas()
    print(df)




 
    