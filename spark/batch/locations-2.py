from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("MergeLocations") \
        .getOrCreate()

    # Read data from output/locations folder
    output_locations_df = spark.read.csv("output/locations", header=True)

    # Read data from streaming/locations folder
    streaming_locations_df = spark.read.csv("streaming/locations", header=True)

    # Union the dataframes and ensure distinct values
    merged_locations_df = output_locations_df.union(streaming_locations_df).distinct()

    # Write the merged dataframe back to streaming/locations folder
    merged_locations_df.write.csv("streaming/locations", mode="overwrite", header=True)

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
