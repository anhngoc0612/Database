from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pymongo import MongoClient
import logging

# Logging Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("KafkaFlightConsumer")

def create_spark_session():
    return SparkSession.builder \
        .appName("KafkaFlightConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config("spark.mongodb.output.uri", "mongodb://root:example@mongodb:27017/mydb.mycollection?authSource=admin") \
        .getOrCreate()

def get_flight_schema():
    return StructType([
        StructField("flights", ArrayType(
            StructType([
                StructField("actual_departure_time", StringType()),
                StructField("actual_landed_time", StringType()),
                StructField("airline", StringType()),
                StructField("arrival_airport", StringType()),
                StructField("arrival_city", StringType()),
                StructField("current_altitude_m", DoubleType()),
                StructField("current_location", StructType([
                    StructField("latitude", DoubleType()),
                    StructField("longitude", DoubleType())
                ])),
                StructField("current_speed_km_h", DoubleType()),
                StructField("departure_airport", StringType()),
                StructField("departure_city", StringType()),
                StructField("dest_lat", DoubleType()),
                StructField("dest_lon", DoubleType()),
                StructField("direction", DoubleType()),
                StructField("distance_travelled", DoubleType()),
                StructField("distance_travelled_km", DoubleType()),
                StructField("flight_status", StringType()),
                StructField("id", IntegerType()),
                StructField("lat", DoubleType()),
                StructField("lon", DoubleType()),
                StructField("scheduled_arrival_time", StringType()),
                StructField("scheduled_departure_time", StringType()),
                StructField("speed", DoubleType()),
                StructField("start_time", StringType())
            ])
        ))
    ])

def process_batch(df, epoch_id):
    # Check if the DataFrame is empty
    if df.count() > 0:
        logger.info(f"[Epoch {epoch_id}] Processing {df.count()} rows.")
        try:
            # Write the DataFrame to MongoDB using the Spark connector
            df.write.format("mongo").mode("append").save()
            logger.info(f"[Epoch {epoch_id}] Successfully written {df.count()} rows to MongoDB.")
        except Exception as e:
            logger.error(f"[Epoch {epoch_id}] MongoDB write failed: {e}")
    else:
        logger.info(f"[Epoch {epoch_id}] No data in this batch.")

def main():
    try:
        spark = create_spark_session()
        schema = get_flight_schema()

        # Consume Kafka stream data
        raw_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "broker:29092") \
            .option("subscribe", "flight-producer") \
            .option("startingOffsets", "earliest") \
            .load()

        # Parse Kafka data and extract the relevant fields
        parsed_df = raw_df.select(from_json(col("value").cast("string"), schema).alias("data"))
        exploded_df = parsed_df.select(explode(col("data.flights")).alias("flight"))
        flight_df = exploded_df.select("flight.*")

        # Start the stream and process each batch
        query = flight_df.writeStream \
            .foreachBatch(process_batch) \
            .outputMode("append") \
            .start()

        query.awaitTermination()

    except Exception as e:
        logger.error(f"Streaming error: {e}")

if __name__ == "__main__":
    main()
