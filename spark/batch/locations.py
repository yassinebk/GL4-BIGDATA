from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("LocationsMentioned") \
    .getOrCreate()

# Load the dataset
df = spark.read.csv("input/linkedin_tech_jobs.csv", header=True)


# Extract distinct locations
locations = df.select("location").distinct()

# Write output to a directory
locations.write.csv("output/locations_mentioned", header=True)

# Stop SparkSession
spark.stop()
