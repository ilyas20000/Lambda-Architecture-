from datetime import datetime, timedelta
import pandas as pd
from pyhive import hive
import pyarrow as pa
import pyarrow.parquet as pq


def batch_proc_1():
        # Hive configuration
    hive_host = 'hive-server'
    hive_port = 10000
    hive_database = 'finnhub_db'
    hive_table_name = 'finnhub_table'
    data = []

    connection = hive.connect(host=hive_host, port=hive_port, database=hive_database)
    cursor = connection.cursor()



    get_query = f"""
                SELECT *
                FROM {hive_table_name}
            """
    

    cursor.execute(get_query)
        # Custom function to filter non-string values
    
    

    for row in cursor.fetchall():
        result_dict = {'p': row[0],  'crypto_pair': row[2], 't': pd.to_datetime(row[3], unit='ms'), 'v': row[4]}
        

        data.append(result_dict)

    

    # Create a DataFrame
    df = pd.DataFrame(data)
    print(df)

   


    

    # Filter rows with non-None crypto_pair
    df_filtered = df[df['crypto_pair'].notna()]

    # Assuming df_filtered is your DataFrame
    df_filtered = df_filtered[df_filtered['crypto_pair'] != '1']


    # Group by crypto_pair and volume
    grouped_data = df_filtered.groupby(['crypto_pair']).mean()


    # Display the result
    print(grouped_data)

    try:
        # Use the open function with 'x' mode to create the file if it doesn't exist
        with open('/tmp/batchViews/parquet_output_1.parquet', 'x'):
            pass  # This block is empty, creating an empty file

        print(f"File '/tmp/batchViews/parquet_output_1.parquet' created successfully.")
    except FileExistsError:
        print(f"File '/tmp/batchViews/parquet_output_1.parquet' already exists.")

    # Write the result to a Parquet file
    parquet_file_path = '/tmp/batchViews/parquet_output_1.parquet'
    # Explicitly cast the timestamp column to microseconds
    grouped_data['t'] = grouped_data['t'].dt.floor('us')
    table = pa.Table.from_pandas(grouped_data)
    pq.write_table(table, parquet_file_path)

    #read data
    df = pq.read_pandas('/tmp/batchViews/parquet_output_1.parquet').to_pandas()
    print(df)



