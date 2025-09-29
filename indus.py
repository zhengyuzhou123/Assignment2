from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FixIndustries").getOrCreate()

# Load the CSV file into a Spark DataFrame
df = spark.read.option("header", "true").option("multiLine", "true") \
    .option("escape", "\"").csv("data/lightcast_job_postings.csv")

# Industry DataFrame
industries = df.select(
    df["NAICS_2022_6"].alias("INDUSTRY_ID"),
    "NAICS_2022_6_NAME",
    "SOC_5",
    "SOC_5_NAME",
    "LOT_SPECIALIZED_OCCUPATION_NAME",
    "LOT_OCCUPATION_GROUP"
).dropDuplicates(["INDUSTRY_ID"])

industries.createOrReplaceTempView("industries")

#
industries.write.mode("overwrite").option("header", "true").csv("tables/industries")