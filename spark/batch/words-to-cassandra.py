from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col, desc, length, current_timestamp,count
from pyspark import SparkContext

# Create a SparkSession
spark = SparkSession.builder \
    .appName("MostRepeatedWords") \
    .master("local[1]") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

# Create a SparkContext
sc = SparkContext.getOrCreate()

# Load the dataset from HDFS
hdfs_path = "input/*.csv"
rdd = sc.textFile(hdfs_path)

# Convert RDD to DataFrame
df = spark.read.csv(rdd, header=True)

# Collect the column names
columns = df.columns

# Define a function to extract words from each column
def extract_words_from_column(df, column):
    return df.select(explode(split(col(column), " ")).alias("word")) \
             .filter(length(col("word")) >= 4)

# Iterate over columns, extract words, and union the results
words = None
for column in columns:
    if words is None:
        words = extract_words_from_column(df, column)
    else:
        words = words.union(extract_words_from_column(df, column))

# Count the occurrences of each word
word_counts = (words.groupBy("word").agg(
        count("*").alias("mentioned_times"),
        current_timestamp().alias("createdat")
))

# Write the word counts to Cassandra table
word_counts.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="words", keyspace="jobs_stream") \
    .mode("append") \
    .save()

# Stop SparkSession and SparkContext
spark.stop()
sc.stop()
