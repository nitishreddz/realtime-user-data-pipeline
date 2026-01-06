"""
Spark Streaming application for processing Kafka messages and storing data in Cassandra.

This module sets up a Spark Structured Streaming application that consumes user data
from a Kafka topic, transforms it, and writes it to a Cassandra database.
"""

import logging
import uuid
from typing import Optional

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, Session
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def create_keyspace(session: Session) -> None:
    """
    Create a Cassandra keyspace if it doesn't already exist.
    
    Args:
        session (Session): Active Cassandra session.
    """
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    
    logging.info("Keyspace created successfully!")


def create_table(session: Session) -> None:
    """
    Create the users table in Cassandra if it doesn't already exist.
    
    Args:
        session (Session): Active Cassandra session.
    """
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)
    
    logging.info("Table created successfully!")


def create_spark_connection() -> Optional[SparkSession]:
    """
    Create and configure a Spark session with required packages.
    
    Returns:
        Optional[SparkSession]: Configured Spark session or None if creation fails.
    """
    s_conn = None
    
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config(
                'spark.jars.packages',
                'com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,'
                'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3'
            ) \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        
    except Exception as e:
        logging.error(f"Couldn't create the Spark session due to exception: {e}")
    
    return s_conn


def connect_to_kafka(spark_conn: SparkSession) -> Optional[DataFrame]:
    """
    Create a streaming DataFrame that reads from Kafka.
    
    Args:
        spark_conn (SparkSession): Active Spark session.
        
    Returns:
        Optional[DataFrame]: Streaming DataFrame connected to Kafka or None if connection fails.
    """
    spark_df = None
    
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        
        logging.info("Kafka dataframe created successfully")
        
    except Exception as e:
        logging.error(f"Kafka dataframe could not be created because: {e}")
    
    return spark_df


def create_cassandra_connection() -> Optional[Session]:
    """
    Establish a connection to the Cassandra cluster.
    
    Returns:
        Optional[Session]: Active Cassandra session or None if connection fails.
    """
    try:
        # Connect to the Cassandra cluster
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()
        
        logging.info("Cassandra connection created successfully!")
        return cas_session
        
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to: {e}")
        return None


def create_selection_df_from_kafka(spark_df: DataFrame) -> DataFrame:
    """
    Transform the raw Kafka DataFrame into a structured format.
    
    This function parses JSON messages from Kafka, applies schema validation,
    generates unique IDs, and prepares data for Cassandra insertion.
    
    Args:
        spark_df (DataFrame): Raw streaming DataFrame from Kafka.
        
    Returns:
        DataFrame: Transformed DataFrame with structured user data.
    """
    # Define schema for user data
    schema = StructType([
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("postcode", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])
    
    # UDF to generate UUID for each record
    uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())
    
    # Parse JSON and transform data
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')) \
        .select("data.*") \
        .withColumn("id", uuid_udf()) \
        .withColumnRenamed("postcode", "post_code")
    
    logging.info("Selection DataFrame created from Kafka stream")
    
    return sel


if __name__ == "__main__":
    # Create Spark connection
    spark_conn = create_spark_connection()
    
    if spark_conn is not None:
        # Connect to Kafka with Spark connection
        spark_df = connect_to_kafka(spark_conn)
        
        if spark_df is not None:
            selection_df = create_selection_df_from_kafka(spark_df)
            
            # Create Cassandra connection
            session = create_cassandra_connection()
            
            if session is not None:
                create_keyspace(session)
                create_table(session)
                
                logging.info("Streaming is being started...")
                
                # Start streaming query to write data to Cassandra
                streaming_query = (
                    selection_df.writeStream
                    .format("org.apache.spark.sql.cassandra")
                    .option('checkpointLocation', '/tmp/checkpoint')
                    .option('keyspace', 'spark_streams')
                    .option('table', 'created_users')
                    .start()
                )
                
                streaming_query.awaitTermination()
            else:
                logging.error("Failed to create Cassandra session. Exiting.")
        else:
            logging.error("Failed to connect to Kafka. Exiting.")
    else:
        logging.error("Failed to create Spark connection. Exiting.")