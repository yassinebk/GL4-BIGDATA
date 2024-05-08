from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col, desc,length


# Create a SparkSession
spark = SparkSession.builder \
    .appName("MostRepeatedWords") \
    .getOrCreate()

# Load the dataset
df = spark.read.csv("input/job_postings.csv", header=True)


words = df.select(explode(split(col("description"), " ")).alias("word")) \
          .filter(length(col("word")) >= 4)

# Count the occurrences of each word
word_counts = words.groupBy("word").count()

# Get the most repeated words in descending order
most_repeated_words = word_counts.orderBy(desc("count"))

most_repeated_words.show()

# Write output to a directory
most_repeated_words.write.csv("output/most_repeated_words", header=True, mode='overwrite')

# Stop SparkSession
spark.stop()
