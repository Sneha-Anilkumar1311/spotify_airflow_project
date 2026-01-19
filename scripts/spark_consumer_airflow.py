from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

# Define schema
schema = StructType() \
    .add("track_name", StringType()) \
    .add("artist", StringType()) \
    .add("album", StringType()) \
    .add("played_at", StringType())

# Create Spark session
spark = SparkSession.builder \
    .appName("spotify_streaming_airflow") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4,"
        "org.postgresql:postgresql:42.7.3"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read from Kafka (STREAMING)
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "spotify_topic") \
    .option("startingOffsets", "latest") \
    .load()

# Parse Kafka JSON value
spotify_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Remove duplicates
spotify_df = spotify_df.dropDuplicates(
    ["track_name", "artist", "played_at"]
)

# Function to write each batch to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/spotifydb") \
        .option("dbtable", "spotify_tracks") \
        .option("user", "postgres") \
        .option("password", "newpassword") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# Start streaming query
query = spotify_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spotify_checkpoint") \
    .start()

query.awaitTermination(60)  # 1 minute
query.stop()


