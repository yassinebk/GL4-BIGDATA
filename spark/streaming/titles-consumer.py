from pyspark.sql import SparkSession

# Create a SparkSession
spark = (
    SparkSession.builder.master("local[1]")
    .appName("Titles Consumer")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .getOrCreate()
)

titles_data = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "hadoop-master:9092")
    .option("subscribe", "titles_topic")
    .option("startingOffsets", "earliest")
    .load()
)

# Extract the title value and remove duplicates
unique_titles = titles_data.selectExpr("CAST(value AS STRING) AS title").distinct()

# Write the unique titles to a file
titles_output = (
    unique_titles.writeStream.format("text")
    .outputMode("append")
    .option("path", "output/titles")
    .option("checkpointLocation", "checkpoint/titles_consumer")
    .start()
)

# Wait for the streaming query to terminate
titles_output.awaitTermination()
