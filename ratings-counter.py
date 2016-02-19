from pyspark import SparkConf, SparkContext
import time


conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)

lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
start = time.time()

# user_id movie_id rating
ratings = lines.map(lambda x: (int(x.split()[1]), int(x.split()[2])))
# (movie, rating)
rdd = ratings.mapValues(lambda x: (x, 1))
# (movie, (rating,1))
# (movie, (rating,1))
result = rdd.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))

res = result.mapValues(lambda x: x[0]/float(x[1]))

final = res.collect()
end = time.time()
for fin in final:
    print fin
print end - start
# 1662 - 2,5
# 1663 - 2
# 1664 - 2,...
