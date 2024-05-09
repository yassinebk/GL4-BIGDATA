from pyspark.sql import SparkSession
from pyspark.sql.functions import count, current_timestamp, col
from pyspark.sql.types import StringType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("LocationsMentioned") \
    .master("local[1]")\
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

# Load the datasets
df1 = spark.read.csv("input/linkedin_tech_jobs.csv", header=True)
df2 = spark.read.csv("input/job_data_merged.csv", header=True)
df3 = spark.read.csv("input/data.csv", header=True)

# Combine all dataframes and select the 'location' column
locations_df = df1.select("location").union(df2.select("Location")).union(df3.select("Location"))

# Filter out null locations
locations_df = locations_df.filter(col("location").isNotNull())

# Aggregate the data to count the number of mentions for each location
location_counts = (
    locations_df.groupBy("location")
    .agg(
        count("*").alias("mentioned_times"),
        current_timestamp().alias("createdat")
    )
)

# Write the aggregated data to Cassandra
location_counts.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="jobs_locations", keyspace="jobs_stream") \
    .mode("append") \
    .save()

# Stop SparkSession
spark.stop()
