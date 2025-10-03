from pyspark.sql import SparkSession

# Start a Spark session
spark = SparkSession.builder.appName("JobPostingsAnalysis").getOrCreate()

# Load the CSV file into a Spark DataFrame
df = spark.read.option("header", "true")\
    .option("inferSchema", "true")\
    .option("multiLine","true")\
    .option("escape", "\"")\
    .csv("data/lightcast_job_postings.csv")

df = df.filter(df["ID"].isNotNull() & (df["ID"] != "ID"))

# Show schema
df.printSchema()
df.select("ID", "COMPANY", "NAICS_2022_6", "NAICS_2022_6_NAME", "TITLE_CLEAN").show(5, truncate=False)