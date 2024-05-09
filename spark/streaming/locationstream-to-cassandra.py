from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, max, current_timestamp
from pyspark.sql.types import StringType, StructType, StructField

# Create a SparkSession with Cassandra configuration
spark = (
    SparkSession.builder.master("local[1]")
    .appName("Locations Consumer")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0")
    .config("spark.cassandra.connection.host", "cassandra")
    .config("spark.cassandra.connection.port", "9042")
    .getOrCreate()
)

# Define the schema for the incoming JSON data
json_schema = StructType([
    StructField("location", StringType(), True)
])

# Read streaming data from Kafka
streaming_data = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "hadoop-master:9092")
    .option("subscribe", "locations_topic")
    .option("startingOffsets", "earliest")
    .load()
)

# Parse the incoming data as JSON and extract the location attribute
parsed_data = streaming_data.select(
    from_json(col("value").cast("string"), json_schema).alias("json")
).select("json.location")

# Aggregate the data to count the number of mentions for each location
location_counts = (
    parsed_data.groupBy("location")
    .agg(
        count("*").alias("mentioned_times"),
        max(current_timestamp()).alias("last_mentioned")
    )
)

# Write the aggregated data to Cassandra
locations_output = (
    location_counts.writeStream.format("org.apache.spark.sql.cassandra")
    .outputMode("update")
    .option("keyspace", "jobs_stream")
    .option("table", "jobs_locations")
    .option("checkpointLocation", "checkpoint/locations_consumer")
    .start()
)

# Wait for the streaming query to terminate
locations_output.awaitTermination()