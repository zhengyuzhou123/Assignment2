from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

# Start a Spark session
spark = SparkSession.builder.appName("RelationalTables").getOrCreate()

# Load the CSV file into a Spark DataFrame
df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("escape", "\"") \
    .csv("data/lightcast_job_postings.csv")


#Company DataFrame
companies = df.select(
    df["COMPANY"].alias("COMPANY_ID"),
    "COMPANY",
    "COMPANY_NAME",
    "COMPANY_RAW",
    "COMPANY_IS_STAFFING"
).dropDuplicates(["COMPANY_ID"])

companies.createOrReplaceTempView("companies")


#Location DataFrame
locations = df.select(
    df["LOCATION"].alias("LOCATION_ID"),
    "CITY_NAME",
    "STATE_NAME",
    "COUNTY_NAME",
    "MSA",
    "MSA_NAME"
).dropDuplicates(["LOCATION_ID"])
locations.createOrReplaceTempView("locations")

#Job DataFrame
job_postings= df.select(
    "ID",
    "TITLE_CLEAN",
    df["COMPANY"].alias("COMPANY_ID"),
    df["NAICS_2022_6"].alias("INDUSTRY_ID"),
    "EMPLOYMENT_TYPE_NAME",
    "REMOTE_TYPE_NAME",
    "BODY",
    "MIN_YEARS_EXPERIENCE",
    "MAX_YEARS_EXPERIENCE",
    "SALARY",
    "SALARY_FROM",
    "SALARY_TO",
    df["LOCATION"].alias("LOCATION_ID"),
    "POSTED", 
    "EXPIRED", 
    "DURATION"
)
job_postings.createOrReplaceTempView("job_postings")

print("Companies row:", companies.count())
print("Locations row:", locations.count())
print("Job Postings row:", job_postings.count())

