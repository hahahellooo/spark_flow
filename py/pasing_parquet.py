from pyspark.sql import SparkSession
import sys
from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.functions import explode_outer, col, size

APP_NAME = sys.argv[1]
spark = SparkSession.builder.appName("Dynamic_parcing").getOrCreate()

# spark으로 멀티라인 json파일 읽어오기
jdf = spark.read.option("multiline","true").json('/home/hahahellooo/data/movies_page/year=2015/data.json')

edf = jdf.withColumn("company", explode_outer("companys"))
eedf = edf.withColumn("director", explode_outer("directors"))
sdf = eedf.select("movieCd", "movieNm", "genreAlt", "typeNm", "director", "company")
sdf = sdf.withColumn("directorNm", col("director.peopleNm"))
rdf = sdf.select("movieCd", "movieNm", "genreAlt", "typeNm", "directorNm", "company.companyCd", "company.companyNm")

rdf.show()

rdf.write.mode("overwrite").parquet('/home/hahahellooo/tmp/sparkdata/movdata')

spark.stop()
