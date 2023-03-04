from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Bank Marketing Data Analysis").getOrCreate()

df=spark.read.options(header=True,delimiter=';',inferSchema=True).csv("/home/miles/futurense_hadoop-pyspark/labs/dataset/bankmarket/bankmarketdata.csv")

df.createOrReplaceTempView("Banking")
df1 = spark.sql("select case when age >= 13 and age <=19 then 'teenagers' when age >= 20 and age <= 30 then 'youngsters' when age >= 31 and age <= 59 then 'middleagers' when age >= 60 then 'senior' end as AgeGroup ,count(*) as counts from Banking where y='yes' group by AgeGroup")
df1.show()
df1.write.format('parquet').save('/home/miles/parquet')

newDF = spark.read.format('parquet').load('/home/miles/parquet')
newDF.show()

newDF.createOrReplaceTempView('newDF_sub2000')

df2 = spark.sql("select * from newDF_sub2000 where counts>2000")
df2.show()

df2.write.format('avro').save('/home/miles/avro')

newDF2 = spark.read.format('avro').load('/home/miles/avro')
newDF2.show()
