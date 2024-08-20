from pyspark.sql import SparkSession
import sys

APP_NAME = sys.argv[1]
spark = SparkSession.builder.appName("select").getOrCreate()

read_df = spark.read.parquet('/home/hahahellooo/tmp/sparkdata/movdata')
read_df.createOrReplaceTempView("movdir")


df3 = spark.sql(f"""
SELECT directorNm, COUNT(*) AS cnt FROM movdir
GROUP BY directorNm
ORDER BY cnt DESC
""")

df3.createOrReplaceTempView("director")

df4 = spark.sql(f"""
SELECT companyNm, COUNT(*) AS cnt FROM movdir
GROUP BY companyNm
ORDER BY cnt DESC
""")
df4.createOrReplaceTempView("company")

print("***영화감독별 작품***")
df3.show()
print("***영화사별 작품 수***")
df4.show()


spark.stop()


