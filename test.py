from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FixJobPostings").getOrCreate()

df = spark.read.option("header", "true") \
    .option("multiLine", "true") \
    .option("escape", "\"") \
    .option("quote", "\"") \
    .csv("data/lightcast_job_postings.csv")

df = df.filter(df["ID"].isNotNull() & (df["ID"] != "ID"))

df.write.mode("overwrite").option("header", "true").csv("tables/job_postings")

print("✅ Fixed job_postings written to tables/job_postings")
