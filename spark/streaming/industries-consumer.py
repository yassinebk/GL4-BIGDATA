from pyspark.sql import SparkSession

# Create a SparkSession
spark = (
    SparkSession.builder.master("local[1]")
    .appName("Industries Collector")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .getOrCreate()
)


industries_data = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "hadoop-master:9092")
    .option("subscribe", "industries_topic")
    .option("startingOffsets", "earliest")
    .load()
)

# Extract the industry value and remove duplicates
unique_industries = industries_data.selectExpr(
    "CAST(value AS STRING) AS industry"
).distinct()

# Write the unique industries to a file
industries_output = (
    unique_industries.writeStream.format("text")
    .outputMode("append")
    .option("path", "output/industries")
    .option("checkpointLocation", "checkpoint/industries_consumer")
    .start()
)

# Wait for the streaming query to terminate
industries_output.awaitTermination()
