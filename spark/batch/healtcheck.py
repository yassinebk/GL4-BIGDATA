from pyspark.sql import SparkSession
from pyspark.sql.functions import upper


# Create a SparkSession
spark = SparkSession.builder.appName("My App Name").getOrCreate()

# Read the CSV file
df = spark.read.csv("input/data.csv", header=True, inferSchema=True)

# Perform a transformation (convert all words to uppercase)
df = df.select([upper(column).alias(column) for column in df.columns])

# Write the result to a new CSV file
df.write.csv("output", header=True)

# Stop the SparkSession
spark.stop()
