from pyspark import SparkContext
sc = SparkContext('local[*]','MinMaxWeather Pyspark App')
rdd1 = sc.textFile('/home/miles/futurense_hadoop-pyspark/labs/dataset/weather/weather_fine.txt')
rdd2=rdd1.map(lambda x:x.split(' ')[5])
rdd3=rdd1.map(lambda x:x.split(' ')[6])
max = rdd2.reduce(lambda a,b:max(a,b))
min = rdd3.reduce(lambda a,b:min(a,b))
print('max_temp ', max)
print('min_temp ', min)

