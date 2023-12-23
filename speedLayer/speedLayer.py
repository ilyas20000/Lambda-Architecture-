from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, LongType
import psycopg2
from psycopg2 import sql

def write_to_postgres(dataframe, table_name):
    # Configure the PostgreSQL connection details
    db_params = {
        'host': 'postgres',
        'database': 'airflow',
        'user': 'airflow',
        'password': 'airflow',
        'port': '5432'
    }
    # Establish a connection to the PostgreSQL database
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    # Create a cursor object to execute SQL queries


    # Convert the Spark DataFrame to a Pandas DataFrame
    pandas_df = dataframe.toPandas()

    # Write the Pandas DataFrame to PostgreSQL
    #pandas_df.to_sql(table_name, conn, index=False, if_exists='append')

    insert_query = sql.SQL("""
            INSERT INTO your_postgres_table (type,p,s,t,v)
            VALUES (%s, %s,%s, %s,%s)
            ON CONFLICT DO NOTHING;
        """)

    for index, row in pandas_df.iterrows():
        cursor.execute(insert_query, (row['type'], row['p'], row['s'], row['t'], row['v']))
        print(row['type'], row['p'], row['s'], row['t'], row['v'])
        connection.commit()

    # Commit the changes and close the connection

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
        .option("subscribe", "finnhub_topic") \
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

    # Write the stream data to a temporary table named 'speed'
    query = df3.writeStream \
        .outputMode("append") \
        .format("memory") \
        .queryName("speed") \
        .start()
    df_speed = spark.sql("SELECT * FROM speed")

    write_to_postgres(df_speed, "your_postgres_table")

    # Wait for the termination of the query
    query.awaitTermination()

    # After the query has terminated, retrieve the data from the temporary table
    df_speed = spark.sql("SELECT * FROM speed")

    write_to_postgres(df_speed, "your_postgres_table")

if __name__ == "__main__":
    main()
