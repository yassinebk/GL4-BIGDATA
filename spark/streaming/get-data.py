from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, regexp_replace, udf, explode
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Define the schema for the JSON data
schema = ArrayType(
    StructType(
        [
            StructField("publishedAt", StringType(), True),
            StructField("salary", StringType(), True),
            StructField("title", StringType(), True),
            StructField("jobUrl", StringType(), True),
            StructField("companyName", StringType(), True),
            StructField("companyUrl", StringType(), True),
            StructField("location", StringType(), True),
            StructField("postedTime", StringType(), True),
            StructField("applicationsCount", StringType(), True),
            StructField("description", StringType(), True),
            StructField("contractType", StringType(), True),
            StructField("experienceLevel", StringType(), True),
            StructField("workType", StringType(), True),
            StructField("sector", StringType(), True),
            StructField("companyId", StringType(), True),
            StructField("posterProfileUrl", StringType(), True),
            StructField("posterFullName", StringType(), True),
        ]
    )
)

# Create a SparkSession
spark = (
    SparkSession.builder.master("local[1]")
    .appName("JobDataProcessor")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .getOrCreate()
)

# Read the streaming data from Kafka
streaming_data = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "hadoop-master:9092")
    .option("subscribe", "job_data_topic")
    .option("startingOffsets", "earliest")
    .load()
)

parsed_data = streaming_data.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select(explode("data").alias("job"))

# Clean the data by removing newlines and extra characters
cleaned_data = parsed_data.withColumn(
    "description", regexp_replace(col("job.description"), "\n|\r", " ")
).withColumn("description", regexp_replace(col("job.description"), "\\s+", " "))

# Extract valuable information
extracted_data = cleaned_data.select(
    col("job.title"),
    col("job.companyName"),
    col("job.salary"),
    col("job.jobUrl"),
    col("job.companyUrl"),
    col("job.location"),
    col("job.postedTime"),
    col("job.sector"),
    col("job.description"),
)


# Define a UDF to decode Unicode characters
def decode_unicode(text):
    if text:
        return text.encode("utf-8").decode("unicode-escape")
    return text


decode_udf = udf(decode_unicode, StringType())

# Apply the UDF to decode Unicode characters in the relevant columns
decoded_data = (
    extracted_data.withColumn("title", decode_udf(col("title")))
    .withColumn("companyName", decode_udf(col("companyName")))
    .withColumn("location", decode_udf(col("location")))
    .withColumn("description", decode_udf(col("description")))
)

processed_data = (
    decoded_data.writeStream.format("json")
    .outputMode("append")
    .option("path", "full_data")
    .option("checkpointLocation", "checkpoint/streaming/linkedin")
    .start()
)

# Write job titles to job_titles_topic
job_titles_data = extracted_data.select("title")
job_titles_processed = (
    job_titles_data.selectExpr("title AS value")
    .writeStream.format("kafka")
    .option("kafka.bootstrap.servers", "hadoop-master:9092")
    .option("topic", "titles_topic")
    .option("checkpointLocation", "checkpoint/job_titles")
    .start()
)

# # Write locations to locations_topic
locations_data = extracted_data.select("location")
locations_processed = (
    locations_data.selectExpr("location AS value")
    .writeStream.format("kafka")
    .option("kafka.bootstrap.servers", "hadoop-master:9092")
    .option("topic", "locations_topic")
    .option("checkpointLocation", "checkpoint/locations")
    .start()
)

# # Write sectors to industries_topic
industries_data = extracted_data.select("sector")
industried_processed = (
    industries_data.selectExpr("sector AS value")
    .writeStream.format("kafka")
    .option("kafka.bootstrap.servers", "hadoop-master:9092")
    .option("topic", "industries_topic")
    .option("checkpointLocation", "checkpoint/industries")
    .start()
)

industried_processed.awaitTermination()
locations_processed.awaitTermination()
job_titles_processed.awaitTermination()
processed_data.awaitTermination()

# Stop the SparkSession
spark.stop()
