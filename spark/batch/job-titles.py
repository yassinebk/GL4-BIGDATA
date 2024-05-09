from pyspark.sql import SparkSession
from pyspark.sql.functions import length

# Create a SparkSession
spark = SparkSession.builder.appName("Job Titles").getOrCreate()

# Load the dataset
df = spark.read.csv("input/job_postings.csv", header=True)
df2 = spark.read.csv("input/data.csv", header=True)

# Filter titles with less than 40 characters
filtered_titles = (
    df.filter(length(df["title"]) < 40)
    .select("title")
    .union(df2.filter(length(df2["Title"]) < 40).select("Title"))
)

filtered_titles.show()

# Write output to a directory
filtered_titles.write.csv("output/job_titles", mode="overwrite", header=True)

# Stop SparkSession
spark.stop()
