from pyspark.sql import SparkSession
import plotly.express as px

# 启动 Spark
spark = SparkSession.builder.appName("IndustrySalaryTrends").getOrCreate()

# 读四张表
industries = spark.read.option("header", "true").csv("tables/industries")
job_postings = spark.read.option("header", "true").csv("tables/job_postings")

# 注册成 SQL 表
industries.createOrReplaceTempView("industries")
job_postings.createOrReplaceTempView("job_postings")

print("Industries count:", industries.count())
print("Job postings count:", job_postings.count())

print("Industries first 5 rows:")
industries.show(5, truncate=False)

print("Job postings first 5 rows:")
job_postings.show(5, truncate=False)

#SQL 查询: 518210 Technology industry
# 检查 518210 行业下有多少个 specialized occupation
check_query = """
SELECT 
    DISTINCT i.LOT_SPECIALIZED_OCCUPATION_NAME
FROM job_postings j
JOIN industries i
    ON j.INDUSTRY_ID = i.INDUSTRY_ID
WHERE i.INDUSTRY_ID = '518210'
"""
check_result = spark.sql(check_query)

print("518210 行业里的不同 Specialized Occupations 数量:", check_result.count())
check_result.show(20, truncate=False)
'''query = """
SELECT 
    i.NAICS_2022_6_NAME AS industry_name,
    i.LOT_SPECIALIZED_OCCUPATION_NAME AS specialized_occupation,
    PERCENTILE_APPROX(
        (TRY_CAST(j.SALARY_FROM AS DOUBLE) + TRY_CAST(j.SALARY_TO AS DOUBLE)) / 2,
        0.5
    ) AS median_salary
FROM job_postings j
JOIN industries i
    ON j.INDUSTRY_ID = i.INDUSTRY_ID
WHERE j.INDUSTRY_ID = '518210'
  AND TRY_CAST(j.SALARY_FROM AS DOUBLE) IS NOT NULL
  AND TRY_CAST(j.SALARY_TO AS DOUBLE) IS NOT NULL
  AND (TRY_CAST(j.SALARY_FROM AS DOUBLE) + TRY_CAST(j.SALARY_TO AS DOUBLE)) / 2 > 0
GROUP BY i.NAICS_2022_6_NAME, i.LOT_SPECIALIZED_OCCUPATION_NAME
ORDER BY median_salary DESC
"""

result = spark.sql(query)
result.show(truncate=False)

# 转成 Pandas 给 Plotly 用
pdf = result.toPandas()

# Plotly 分组柱状图
fig = px.bar(
    pdf,
    x="median_salary",
    y="specialized_occupation",
    color="industry_name",
    orientation="h",
    title="Median Salary Trends in the Technology Sector by Specialized Occupation",
    labels={"specialized_occupation": "Specialized Occupation", "median_salary": "Median Salary ($)"}
)

fig.show()
fig.write_html("Query1_result.html")'''