from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

def leser(x,y):
	if x>y: return x
	else: return y

lines = sc.textFile("file:///SparkCourse/1800.csv")
lines = lines.filter(lambda x: "TMAX" in x)
data = lines.map(lambda x: (x.split(',')[0], x.split(',')[3]))
# id, temp value
data1 = data.mapValues(lambda x: int(x)*0.1)

data2 = data1.reduceByKey(leser)

final = data2.collect()

for fin in final:
	print fin