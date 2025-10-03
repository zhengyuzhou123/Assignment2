from pyspark.sql import SparkSession

# 启动 Spark
spark = SparkSession.builder.appName("RestructureTables").getOrCreate()

# 读取原始大文件
df = spark.read.option("header", "true")\
    .option("inferSchema", "true")\
    .option("multiLine","true")\
    .option("escape", "\"")\
    .csv("data/lightcast_job_postings.csv")

df = df.filter(df["ID"].isNotNull() & (df["ID"] != "ID"))

# ----------------- Industries -----------------
industries = df.select(
    df["NAICS_2022_6"].alias("INDUSTRY_ID"),
    "NAICS_2022_6_NAME",
    "SOC_5",
    "SOC_5_NAME",
    "LOT_SPECIALIZED_OCCUPATION_NAME",
    "LOT_OCCUPATION_GROUP"
).dropDuplicates(["INDUSTRY_ID"])

industries.createOrReplaceTempView("industries")

industries.write.mode("overwrite") \
    .option("header", "true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("multiLine", "true") \
    .csv("tables/industries")

# ----------------- Companies -----------------
companies = df.select(
    df["COMPANY"].alias("COMPANY_ID"),
    "COMPANY_NAME",
    "COMPANY_RAW",
    "COMPANY_IS_STAFFING"
).dropDuplicates(["COMPANY_ID"])
companies.createOrReplaceTempView("companies")
companies.write.mode("overwrite") \
    .option("header", "true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("multiLine", "true") \
    .csv("tables/companies")


# ----------------- Locations -----------------
locations = df.select(
    df["LOCATION"].alias("LOCATION_ID"),
    "CITY_NAME",
    "STATE_NAME",
    "COUNTY_NAME",
    "MSA",
    "MSA_NAME"
).dropDuplicates(["LOCATION_ID"])
locations.createOrReplaceTempView("locations")
locations.write.mode("overwrite") \
    .option("header", "true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("multiLine", "true") \
    .csv("tables/locations")

# ----------------- Job Postings -----------------
job_postings = df.select(
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
job_postings = job_postings.dropna(subset=["ID", "INDUSTRY_ID", "COMPANY_ID"])
job_postings.createOrReplaceTempView("job_postings")
job_postings.write.mode("overwrite") \
    .option("header", "true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("multiLine", "true") \
    .csv("tables/job_postings")

# 打印行数确认
print("Industries rows:", industries.count())
print("Companies rows:", companies.count())
print("Locations rows:", locations.count())
print("Job Postings rows:", job_postings.count())