from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a SparkSession
spark = SparkSession.builder \
    .appName("RemoteWorkCount") \
    .getOrCreate()

# Load the dataset
df = spark.read.csv("input/job_postings.csv", header=True)

# Count the number of job offers that allow remote work
remote_work_count = df.filter(col("remote_allowed") == "1.0").count()

print(remote_work_count)

# Stop SparkSession
spark.stop()
