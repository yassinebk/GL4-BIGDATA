from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("LocationsMentioned") \
    .getOrCreate()

# Load the dataset
df = spark.read.csv("input/linkedin_tech_jobs.csv", header=True)
df2 = spark.read.csv("input/job_data_merged.csv", header=True)
df3 = spark.read.csv("input/linkedin_tech_jobs.csv", header=True)
df4 = spark.read.csv("input/data.csv", header=True)




# Extract distinct locations
locations = df.select("location").distinct().union(df2.select("Location").distinct()).union(df3.select("Location").distinct()).union(df4.select("Location").distinct())

# Write output to a directory
locations.write.csv("output/locations", header=True, mode="overwrite")

# Stop SparkSession
spark.stop()
