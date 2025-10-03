from pyspark.sql import SparkSession

# 启动 Spark
spark = SparkSession.builder.appName("RemoteJobsCalifornia").getOrCreate()

# 读取表
companies = spark.read.csv("tables/companies", header=True, inferSchema=True)
locations = spark.read.csv("tables/locations", header=True, inferSchema=True)
job_postings = spark.read.csv("tables/job_postings", header=True, inferSchema=True)

# 注册成临时视图
companies.createOrReplaceTempView("companies")
locations.createOrReplaceTempView("locations")
job_postings.createOrReplaceTempView("job_postings")

# ✅ JOIN locations，拿 STATE_NAME
joined = job_postings.join(
    locations,
    job_postings["LOCATION_ID"].cast("string") == locations["LOCATION_ID"].cast("string"),
    "inner"  # inner 避免 NULL
)

# 选择需要的列
result = joined.select(
    job_postings["ID"],
    job_postings["TITLE_CLEAN"],
    job_postings["REMOTE_TYPE_NAME"],
    locations["STATE_NAME"]
)

result.show(20, truncate=False)

spark.stop()