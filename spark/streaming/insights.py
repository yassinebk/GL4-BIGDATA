from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a SparkSession
spark = SparkSession.builder.appName("RealTimeDataProcessor").getOrCreate()

# Read the streaming data from the output directory of the first application
streaming_data = (
    spark.readStream.format("json")
    .option("maxFilesPerTrigger", 1)
    .load("output/streaming/linkedin/")
)

# Define your processing logic here
processed_data = streaming_data.select(
    col("salary"),
    col("title"),
    col("companyName"),
    col("location"),
    col("postedTime"),
    col("description"),
)

# Write the processed data to a sink (e.g., console, file, etc.)
# You can replace "console" with "file" or any other supported sink
query = processed_data.writeStream.format("console").outputMode("append").start()

# Wait for the streaming query to terminate
query.awaitTermination()

# Stop the SparkSession
spark.stop()
