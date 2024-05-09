from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col, desc, length
from pyspark import SparkContext

# Create a SparkSession
spark = SparkSession.builder \
    .appName("MostRepeatedWords") \
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
word_counts = words.groupBy("word").count()

# Get the most repeated words in descending order
most_repeated_words = word_counts.orderBy(desc("count"))

most_repeated_words.show()

# Write output to HDFS
output_path = "output/most_repeated_words"
most_repeated_words.write.csv(output_path, header=True, mode='overwrite')

# Stop SparkSession and SparkContext
spark.stop()
sc.stop()
