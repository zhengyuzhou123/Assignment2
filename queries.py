from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("SQLQueries").getOrCreate()

# 读取 industries.csv
industries = spark.read.option("header", "true").csv("tables/industries")  # 注意是目录
industries.createOrReplaceTempView("industries")

# 读取 job_postings
job_postings = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("escape", "\"") \
    .csv("data/lightcast_job_postings.csv") \
    .select("ID", "NAICS_2022_6")

job_postings = job_postings.withColumnRenamed("NAICS_2022_6", "INDUSTRY_ID") \
    .withColumn("INDUSTRY_ID", col("INDUSTRY_ID").cast("string"))

# ✅ 注册临时表
job_postings.createOrReplaceTempView("job_postings")

# 查询
query = """
SELECT i.NAICS_2022_6_NAME, COUNT(*) AS postings_count
FROM job_postings j
JOIN industries i
ON j.INDUSTRY_ID = i.INDUSTRY_ID
WHERE i.NAICS_2022_6_NAME IS NOT NULL
GROUP BY i.NAICS_2022_6_NAME
ORDER BY postings_count DESC
LIMIT 5
"""

result = spark.sql(query)
result.show(truncate=False)

print("Job postings schema:", job_postings.printSchema())
print("Industries schema:", industries.printSchema())