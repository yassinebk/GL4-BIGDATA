from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("JobOffersByCompany") \
    .getOrCreate()

# Load the dataset
df = spark.read.csv("input/nyc-jobs.csv", header=True)



# Group by company_id and count the number of job offers
job_offers_by_company = df.groupBy("company_id").count()

# Write output to a directory
job_offers_by_company.write.csv("output/job_offers_by_company", header=True)

# Stop SparkSession
spark.stop()
