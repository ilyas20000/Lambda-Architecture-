from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, explode, expr, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, LongType
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import psycopg2
from psycopg2 import sql

current_p_btc = 0.0
current_p_eth = 0.0
prv_p_aapl = 0.0
prv_p_amz = 0.0
prv_p_msft = 0.0
prv_p_gbp = 0.0
def process_batch(batch_df, epoch_id):
    global current_p_btc, current_p_eth,prv_p_aapl,prv_p_msft,prv_p_gbp,prv_p_amz
    db_params = {
        'host': 'postgres',
        'database': 'airflow',
        'user': 'airflow',
        'password': 'airflow',
        'port': '5432'
    }
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()


    # Iterate through the batch DataFrame
    for row in batch_df.collect():

        current_s = row['s']
        current_p = row['p']
        # Update variables based on 's'
        if (current_s == 'BINANCE:BTCUSDT' and current_p != current_p_btc):
            if current_p_btc == 0 :
                current_p_btc = row['p']
            try:
                update_query = """
                                    UPDATE speed_table
                                    SET type = %s, p = %s, s = %s, t = %s, v = %s, percentage_change = %s , absolute_price_change = %s
                                    WHERE s = 'BINANCE:BTCUSDT';
                                """
                cursor.execute(update_query, (row['type'], row['p'], row['s'], row['t'], row['v'], calculate_percentage_change(current_p,current_p_btc),calculate_absolute_price_change(current_p,current_p_btc)))

            except Exception as e:
                print(f"Error: {e}")
            finally:

                connection.commit()
                current_p_btc = current_p
        elif current_s == 'AAPL' and current_p != current_p_eth:
            if prv_p_aapl == 0:
               prv_p_aapl = row['p']
            try:
                update_query = """
                    UPDATE speed_table
                    SET type = %s, p = %s, s = %s, t = %s, v = %s, percentage_change = %s , absolute_price_change = %s
                    WHERE s = 'AAPL';
                """
                cursor.execute(update_query, (row['type'], row['p'], row['s'], row['t'], row['v'], calculate_percentage_change(current_p,prv_p_aapl),calculate_absolute_price_change(current_p,prv_p_aapl)))

            except Exception as e:
                print(f"Error: {e}")
            finally:

                connection.commit()
                prv_p_aapl = current_p
        elif current_s == 'AMZN' and current_p != current_p_eth:
            if prv_p_amz == 0:
               prv_p_amz = row['p']
            try:
                update_query = """
                    UPDATE speed_table
                    SET type = %s, p = %s, s = %s, t = %s, v = %s, percentage_change = %s , absolute_price_change = %s
                    WHERE s = 'AMZN';
                """
                cursor.execute(update_query, (row['type'], row['p'], row['s'], row['t'], row['v'], calculate_percentage_change(current_p,prv_p_amz),calculate_absolute_price_change(current_p,prv_p_amz)))

            except Exception as e:
                print(f"Error: {e}")
            finally:

                connection.commit()
                prv_p_amz = current_p
        elif current_s == 'MSFT' and current_p != current_p_eth:
            if prv_p_msft == 0:
               prv_p_msft = row['p']
            try:
                update_query = """
                    UPDATE speed_table
                    SET type = %s, p = %s, s = %s, t = %s, v = %s, percentage_change = %s , absolute_price_change = %s
                    WHERE s = 'MSFT';
                """
                cursor.execute(update_query, (row['type'], row['p'], row['s'], row['t'], row['v'], calculate_percentage_change(current_p,prv_p_msft),calculate_absolute_price_change(current_p,prv_p_msft)))

            except Exception as e:
                print(f"Error: {e}")
            finally:

                connection.commit()
                prv_p_msft = current_p
        elif current_s == 'GBP' and current_p != current_p_eth:
            if prv_p_gbp == 0:
               prv_p_gbp = row['p']
            try:
                update_query = """
                    UPDATE speed_table
                    SET type = %s, p = %s, s = %s, t = %s, v = %s, percentage_change = %s , absolute_price_change = %s
                    WHERE s = 'GBP';
                """
                cursor.execute(update_query, (row['type'], row['p'], row['s'], row['t'], row['v'], calculate_percentage_change(current_p,prv_p_gbp),calculate_absolute_price_change(current_p,prv_p_gbp)))

            except Exception as e:
                print(f"Error: {e}")
            finally:

                connection.commit()
                prv_p_gbp = current_p
        elif current_s == 'BINANCE:ETHUSDT' and current_p != current_p_eth:
            if current_p_eth == 0:
               current_p_eth = row['p']
            try:
                update_query = """
                    UPDATE speed_table
                    SET type = %s, p = %s, s = %s, t = %s, v = %s,percentage_change = %s , absolute_price_change = %s
                    WHERE s = 'BINANCE:ETHUSDT';
                """
                cursor.execute(update_query, (row['type'], row['p'], row['s'], row['t'], row['v'], calculate_percentage_change(current_p,current_p_eth),calculate_absolute_price_change(current_p,current_p_eth)))

            except Exception as e:
                print(f"Error: {e}")
            finally:

                connection.commit()
                current_p_eth = current_p






def calculate_absolute_price_change(current_value, previous_value):
    # return ((previous_value - current_value ) / previous_value) * 100
    return  current_value - previous_value
def calculate_percentage_change(current_value, previous_value):
    return ((previous_value - current_value ) / previous_value) * 100

def main():
    # Create a Spark session
    spark = SparkSession.builder.appName("KafkaSparkStreamingFinnhub").getOrCreate()

    # Define the schema to match the JSON structure
    schema = StructType([
        StructField("data", ArrayType(StructType([
            StructField("p", DoubleType(), True),
            StructField("s", StringType(), True),
            StructField("t", LongType(), True),
            StructField("v", DoubleType(), True),
        ])), True),
        StructField("type", StringType(), True),
    ])

    # Subscribe to 1 topic
    df1 = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "speed_topic") \
        .option("group.id", "finnhub") \
        .option("partition.assignment.strategy", "range") \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON and explode the array of data
    df2 = df1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .select(from_json("value", schema).alias("json")) \
        .select("json.*") \
        .select("type", explode("data").alias("data"))

    # Select the specific fields you are interested in
    df3 = df2.selectExpr("type", "data.p AS p", "data.s AS s", "data.t AS t", "data.v AS v")

    
    df3.writeStream \
        .foreachBatch(lambda df, epochId: process_batch(df, epochId)) \
        .outputMode("append") \
        .format("console") \
        .start() \
        .awaitTermination()
if __name__ == "__main__":

    main()
