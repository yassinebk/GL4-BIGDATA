from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "ApacheSparkStreamingTest")
ssc = StreamingContext(sc, 1)

# Create a DStream that will connect to a TCP socket
lines = ssc.socketTextStream("localhost", 9999)

# Split each line into words
words = lines.flatMap(lambda line: line.split(" "))

# Count each word in each batch
word_counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

# Print the word counts
word_counts.pprint()

# Start the computation
ssc.start()

# Wait for the computation to terminate
ssc.awaitTermination()
