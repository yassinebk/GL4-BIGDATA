from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json,explode,regexp_replace,udf,lower,current_timestamp

spark = SparkSession.builder \
    .master("local[1]")\
    .appName("JobPostingsProcessor") \
    .config("spark.jars.packages","com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")\
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

from pyspark.sql.types import StructType, StructField, StringType, IntegerType,ArrayType

schema = ArrayType(StructType([
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
    StructField("posterFullName", StringType(), True)
]))


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
    lower(col("job.title")).alias("title"),
    lower(col("job.companyName")).alias("companyname"),
    lower(col("job.salary")).alias("salary"),
    lower(col("job.jobUrl")).alias("joburl"),
    lower(col("job.companyUrl")).alias("companyurl"),
    lower(col("job.location")).alias("location"),
    lower(col("job.postedTime")).alias("postedtime"),
    lower(col("job.sector")).alias("sector"),
    lower(col("job.description")).alias("description"),
    current_timestamp().alias("added_at")

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
    .withColumn("companyname", decode_udf(col("companyname")))
    .withColumn("location", decode_udf(col("location")))
    .withColumn("description", decode_udf(col("description")))
)

processed_data = (
    decoded_data.writeStream
    .format("org.apache.spark.sql.cassandra")
    .option("keyspace", "jobs_stream")
    .option("table", "jobs_postings")
    .option("checkpointLocation", "checkpoint/streaming/linkedin")
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

processed_data.awaitTermination()


