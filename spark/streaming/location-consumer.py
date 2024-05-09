from pyspark.sql import SparkSession

# Create a SparkSession
spark = (
    SparkSession.builder.master("local[1]")
    .appName("Locations Consumer")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .getOrCreate()
)

# Read existing locations data from the file
existing_locations_data = spark.read.csv("output/locations/*.csv", header=True)

# Read streaming data from Kafka
streaming_locations_data = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "hadoop-master:9092")
    .option("subscribe", "locations_topic")
    .option("startingOffsets", "earliest")
    .load()
)

# Extract the location value from streaming data
streaming_locations = streaming_locations_data.selectExpr(
    "CAST(value AS STRING) AS location"
)

# Union existing locations data with streaming data
all_locations = existing_locations_data.union(streaming_locations)

# Deduplicate the combined dataset
unique_locations = all_locations.distinct()

# Write the unique locations back to the file
locations_output = (
    unique_locations.writeStream.format("text")
    .outputMode(
        "append"
    )  # Since we are rewriting the entire file each time, use complete mode
    .option("path", "output/locations")
    .option("checkpointLocation", "checkpoint/locations_consumer")
    .start()
)

# Wait for the streaming query to terminate
locations_output.awaitTermination()
